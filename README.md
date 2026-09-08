# KYC-Enriched Next Best Offer

### with Agent-to-Agent Email Outreach on Confluent Cloud & Flink

A hands-on lab that builds a real-time, KYC-aware recommendation pipeline end to end: streaming data generation, Flink SQL enrichment and deterministic risk logic, and two independent Flink Streaming Agents that hand work off to each other through a Kafka topic, the first composing a personalized offer, the second dispatching the outreach email through an MCP tool.

---

## Contents

1. [Use Case](#use-case)
2. [Architecture](#architecture)
3. [Prerequisites](#prerequisites)
4. [Step 1: Set Up the Environment](#step-1-set-up-the-environment)
5. [Step 2: Generate KYC-Rich Mock Data (Datagen)](#step-2-generate-kyc-rich-mock-data-datagen)
6. [Step 3: Configure the Amazon Bedrock Connection and Model](#step-3-configure-the-amazon-bedrock-connection-and-model)
7. [Step 4: Stream Processing - Enrichment and KYC Business Logic](#step-4-stream-processing--enrichment-and-kyc-business-logic)
8. [Step 5: Agent 1 - NBO Recommender](#step-5-agent-1--nbo-recommender-flink-streaming-agent)
9. [Step 6: Provision the Email Dispatch Tool (MCP Server)](#step-6-provision-the-email-dispatch-tool-mcp-server)
10. [Step 7: Agent 2 - Email Dispatch Agent](#step-7-agent-2--email-dispatch-agent-flink-streaming-agent)
11. [Step 8: End-to-End Test](#step-8-end-to-end-test)
12. [Expected Results](#expected-results)

---

## Use Case

A retail bank wants to move beyond simple "buy X, get offer Y" logic. Every time a customer transacts, the bank wants to:

1. Build a rich **KYC (Know Your Customer)** profile in real time, not just demographics, but credit risk, dependents, employment, and account tenure.
2. Use that profile to compute a **deterministic offer quadrant**, the actual risk-based eligibility logic a bank uses to decide who qualifies for what.
3. Use **generative AI** to turn that quadrant into a personalized, on-brand message. This is the enrichment layer AI adds on top of the deterministic logic.
4. Have a second, independent AI agent pick up qualifying offers and autonomously compose and dispatch the outreach email with the two agents communicating asynchronously through a Kafka topic, exactly the way independent microservices would in production.

This workshop builds that whole pipeline end to end:

**Datagen → Flink SQL (enrichment + business logic) → Agent 1 (NBO Recommender) → Kafka topic handoff → Agent 2 (Email Dispatcher) → Email MCP tool.**

---

## Architecture

The pipeline is a linear stream of stages. Each stage reads from the stage before it and writes a new stream that the next stage consumes:

```text
Datagen  (customers, products, transactions)
    |
    v
Flink SQL  -  3-way JOIN + enrichment
    |
    v
Flink SQL  -  deterministic KYC business logic
    (credit_score, risk_tier, dependents, employment_type,
     account_tenure_years  ->  customer_segment,
     target_offer_quadrant, should_email)
    |
    v
Agent 1  -  nbo_recommender_agent  (Flink Streaming Agent)
    reasons over the KYC + offer context  ->  personalized offer text
    |
    v
Kafka topic: next_best_offers      <=== agent-to-agent handoff point
    (rows where should_email = TRUE)
    |
    v
Agent 2  -  email_dispatch_agent  (Flink Streaming Agent)
    composes subject/body  ->  calls the send_email tool
    |
    v
Email MCP tool  -  dispatches the outreach email
```

> **Why two independent agents instead of one big prompt?**  
> Agent 1 and Agent 2 never call each other directly as functions. Agent 1 publishes its decision as an event to a Kafka topic, and Agent 2 reacts to that event on its own schedule. This is the same event-driven, decoupled pattern any two microservices would use - and it means each agent can be scaled, replayed, debugged, and redeployed independently. That is a core benefit of building agents natively on Flink rather than gluing scripts together.

---

## Prerequisites

- A **Confluent Cloud** account with a Basic or Standard cluster and a Flink compute pool provisioned in the same region.
- An **AWS account** with Amazon Bedrock model access enabled for a fast, low-cost Anthropic Claude model. Check the AWS Bedrock console under **Model access** for whichever Claude Haiku-class model is currently available to you. Haiku-class models are recommended here for low latency and low cost on a high-throughput stream.
- **Postman** (optional) if you prefer to provision the Datagen connectors via the REST API rather than the UI.

---

## Step 1: Set Up the Environment

1. Log in to Confluent Cloud.
2. You will create an **Environment** (e.g., `workshop-kyc-nbo`) and a **Cluster** (e.g., `kyc-nbo-cluster`). The steps below walk through both.
3. On the Confluent Cloud home screen, select **Environments** in the left-hand menu.
<img width="2560" height="1230" alt="Screenshot 2026-09-08 at 09 41 15" src="https://github.com/user-attachments/assets/f7bd687c-7460-46f7-9fe1-ccb108aec9c9" />

4. Click **Add cloud environment** on the right side of the page.
<img width="2560" height="1233" alt="image" src="https://github.com/user-attachments/assets/9037476d-6d7f-447f-82c3-d6d257eb895a" />

5. Create an environment with the following specifications:
   - Environment name: `workshop-kyc-nbo`
   - Stream Governance package: **Essentials**
<img width="2560" height="1233" alt="image" src="https://github.com/user-attachments/assets/0b0c880e-806e-4d21-a130-b1b5d91b7544" />

6. Click **Create** to finish setting up the environment.
7. Create a Kafka cluster with the following specifications:
   - Cluster name: `kyc-nbo-cluster`
   - Cluster type: **Basic**
   - Cloud provider: **AWS**
   - Region: **Singapore (ap-southeast-1)**
<img width="2560" height="1215" alt="image" src="https://github.com/user-attachments/assets/4a272de5-bea5-4b85-b122-6e982c3aabb3" />

8. Click **Launch cluster** to create your cluster.

9. Generate a cluster **API Key and Secret**, and save them - you will need them for the Datagen connector configs.
<img width="2560" height="1219" alt="image" src="https://github.com/user-attachments/assets/c3433ea7-3163-4f7a-b894-0cb2e66db1fe" />

<img width="2560" height="1223" alt="image" src="https://github.com/user-attachments/assets/8e6c7760-9d63-4ca4-9efa-1d75d5bfbe21" />

> **Tip**  
> Store the API key and secret somewhere safe as soon as you download them. Confluent Cloud shows the secret only once, and every Datagen connector in Step 2 needs it.

10. Create the topics needed for the workshop. Select **Topics** in the left-hand menu, then click **Create topic**. Name the first topic `customers` and set **Partitions** to `1`.
<img width="2560" height="1222" alt="image" src="https://github.com/user-attachments/assets/8aa1edc9-62c8-4012-88a9-80cf9b2edad7" />
<img width="2560" height="1224" alt="image" src="https://github.com/user-attachments/assets/dd582a0a-5605-4e99-87c5-a6116d34fc63" />

11. If you are prompted to add a data contract, click **Skip**.
<img width="2560" height="1229" alt="image" src="https://github.com/user-attachments/assets/131fac15-d32e-4b9d-86c1-f547305aadeb" />

13. Repeat the previous two steps to create the `products` and `transactions` topics (partitions = 1 each).
14. After creating the topics, now you will create a Flink compute pool in the same region as your cluster. To do that, return to the Clusters page by clicking the **Clusters** link at the top left.
<img width="2560" height="1223" alt="image" src="https://github.com/user-attachments/assets/8bdab0de-8f84-4381-b486-52eb965a9f78" />

16. In the left-hand menu, select **Flink**.
<img width="2560" height="1231" alt="image" src="https://github.com/user-attachments/assets/43ea55e2-8cbe-4c6e-8ea0-77f12fa78c1e" />

18. Click the **Compute pools** tab.
<img width="2560" height="1231" alt="image" src="https://github.com/user-attachments/assets/ded5a6d6-c70f-4dc1-a95c-a1b57782d37d" />

20. Click **Add compute pool**.
<img width="2560" height="1229" alt="image" src="https://github.com/user-attachments/assets/e9035b60-40ab-4789-a3f8-4cbda7016583" />

22. Choose your cloud provider (**AWS**) and region (**Singapore**). The Flink compute pool must match the Kafka cluster's region.
<img width="2560" height="1224" alt="image" src="https://github.com/user-attachments/assets/82db5021-365d-4335-bc10-09cdc8706c32" />

18. Name the pool `kyc_computepool` and set **Max size** to **20 CFU**.
<img width="2560" height="1226" alt="image" src="https://github.com/user-attachments/assets/dc9a497f-8cf7-47fe-a129-a2a37e481a1e" />

20. Click **Create** to create your compute pool.

> **Why the region must match**  
> A Flink compute pool can only read and write Kafka topics that live in the same cloud region. If the pool and the cluster are in different regions, none of the SQL statements in Steps 4–7 will be able to see your topics.

---

## Step 2: Generate KYC-Rich Mock Data (Datagen)

We simulate three streams: `customers` (the KYC profile), `products` (catalog reference data), and `transactions` (live orders). All three use **AVRO** so Confluent Schema Registry can be leveraged.

### Option A - Confluent Cloud UI

Go back to your environment by clicking the environment name at the top left of the page: 
<img width="2560" height="1228" alt="image" src="https://github.com/user-attachments/assets/7f3ea198-565e-4b56-95c6-70b56cd0baac" />

Then click **Clusters** in the left-hand menu.
<img width="2560" height="1232" alt="image" src="https://github.com/user-attachments/assets/ba7e2377-0ca8-4279-b347-fcc94be78c9e" />

Click the cluster you created earlier (`kyc-nbo-cluster`):
<img width="2560" height="1228" alt="image" src="https://github.com/user-attachments/assets/5a75a078-ce48-4f84-9d1c-f7f1f5b901b4" />

Click **Connectors** in the left-hand menu:
<img width="2560" height="1228" alt="image" src="https://github.com/user-attachments/assets/cffd7fae-bc86-4752-9779-f8aa0c114562" />

Navigate to **Connectors → Add Connector → Sample Data (Datagen Source) → Additional Configuration**. Choose the **Custom Schema (AVRO)** option.

**For all three connectors, use these common settings:**

- **Kafka credentials:** Use existing API key → supply the API Key and Secret from Step 1.
- **Select a schema:** Provide your own schema (paste from below).
- **Advanced configuration → Max interval between messages (ms):** `1000`.
- **Tasks:** `1` (the default is fine).
- **Name:** name each connector appropriately (shown below).

#### 1. DatagenSource_Customers connector

- **Topic selection:** `customers`
- **Name:** `DatagenSource_Customers`

**Schema:**

```avro
{
  "type": "record",
  "name": "Customer",
  "fields": [
    {"name": "customer_id", "type": {"type": "string", "arg.properties": {"options": ["customer_1","customer_2","customer_3","customer_4","customer_5","customer_6","customer_7","customer_8","customer_9","customer_10"]}}},
    {"name": "name", "type": {"type": "string", "arg.properties": {"options": ["Andi Wijaya","Siti Rahma","Budi Santoso","Dewi Lestari","Rizky Pratama","Nadia Putri","Fajar Nugroho","Maya Sari","Agus Setiawan","Rina Marlina"]}}},
    {"name": "email", "type": {"type": "string", "arg.properties": {"options": ["andi.wijaya@example.com","siti.rahma@example.com","budi.santoso@example.com","dewi.lestari@example.com","rizky.pratama@example.com","nadia.putri@example.com","fajar.nugroho@example.com","maya.sari@example.com","agus.setiawan@example.com","rina.marlina@example.com"]}}},
    {"name": "gender", "type": {"type": "string", "arg.properties": {"options": ["Male","Female"]}}},
    {"name": "region", "type": {"type": "string", "arg.properties": {"options": ["Jakarta","Jawa Barat","Jawa Tengah","Jawa Timur","Bali","Sumatra Utara","Sulawesi Selatan"]}}},
    {"name": "employment_type", "type": {"type": "string", "arg.properties": {"options": ["Employed","Self-Employed","Business Owner","Student","Retired"]}}},
    {"name": "dependents", "type": {"type": "int", "arg.properties": {"range": {"min": 0, "max": 5}}}},
    {"name": "credit_score", "type": {"type": "int", "arg.properties": {"range": {"min": 300, "max": 850}}}},
    {"name": "risk_tier", "type": {"type": "string", "arg.properties": {"options": ["Low","Medium","High"]}}},
    {"name": "account_tenure_years", "type": {"type": "int", "arg.properties": {"range": {"min": 0, "max": 20}}}},
    {"name": "loyalty_tier", "type": {"type": "string", "arg.properties": {"options": ["Bronze","Silver","Gold","Platinum"]}}}
  ]
}
```

#### 2. DatagenSource_Products connector

- **Topic selection:** `products`
- **Name:** `DatagenSource_Products`

**Schema:**

```avro
{
  "type": "record",
  "name": "Product",
  "fields": [
    {"name": "item_id", "type": {"type": "string", "arg.properties": {"options": ["item_1","item_2","item_3","item_4","item_5","item_6","item_7","item_8","item_9","item_10"]}}},
    {"name": "product_name", "type": {"type": "string", "arg.properties": {"options": ["Wireless Earbuds","Running Shoes","Rice Cooker","Sofa Set","Facial Serum","Motor Oil","Smart TV","Backpack","Blender","Air Fryer"]}}},
    {"name": "category", "type": {"type": "string", "arg.properties": {"options": ["Electronics","Fashion","Groceries","Home & Living","Beauty","Automotive"]}}},
    {"name": "price", "type": {"type": "double", "arg.properties": {"range": {"min": 50000, "max": 5000000}}}},
    {"name": "margin_tier", "type": {"type": "string", "arg.properties": {"options": ["Low","Medium","High"]}}},
    {"name": "inventory_level", "type": {"type": "int", "arg.properties": {"range": {"min": 0, "max": 1000}}}}
  ]
}
```

#### 3. DatagenSource_Transactions connector

- **Topic selection:** `transactions`
- **Name:** `DatagenSource_Transactions`

**Schema:**

```avro
{
  "type": "record",
  "name": "Transaction",
  "fields": [
    {"name": "order_id", "type": {"type": "long", "arg.properties": {"range": {"min": 1, "max": 999999}}}},
    {"name": "customer_id", "type": {"type": "string", "arg.properties": {"options": ["customer_1","customer_2","customer_3","customer_4","customer_5","customer_6","customer_7","customer_8","customer_9","customer_10"]}}},
    {"name": "item_id", "type": {"type": "string", "arg.properties": {"options": ["item_1","item_2","item_3","item_4","item_5","item_6","item_7","item_8","item_9","item_10"]}}},
    {"name": "order_units", "type": {"type": "int", "arg.properties": {"range": {"min": 1, "max": 5}}}},
    {"name": "order_amount", "type": {"type": "double", "arg.properties": {"range": {"min": 10000, "max": 5000000}}}}
  ]
}
```

> **Why the ID pools overlap**  
> The `customer_id` and `item_id` option pools are intentionally the same 10 values across all three connectors. This guarantees that the Flink joins in Step 4 hit real matches instead of dead-ending on random, non-overlapping IDs.

### Option B - Postman (REST API)

If you prefer to provision the connectors programmatically, POST each payload below.

- **URL:** `https://api.confluent.cloud/connect/v1/environments/{{environment_id}}/clusters/{{cluster_id}}/connectors`
- **Method:** `POST`
- **Auth:** Basic Auth using your cluster API Key/Secret (or org-level credentials).
- **Header:** `Content-Type: application/json`

#### 1. Customers Datagen payload (KYC profile)

```json
{
  "name": "DatagenSource_Customers",
  "config": {
    "connector.class": "DatagenSource",
    "name": "DatagenSource_Customers",
    "kafka.api.key": "{{cluster_api_key}}",
    "kafka.api.secret": "{{cluster_api_secret}}",
    "kafka.topic": "customers",
    "output.data.format": "AVRO",
    "max.interval": "1000",
    "tasks.max": "1",
    "schema.string": "{\"type\":\"record\",\"name\":\"Customer\",\"fields\":[{\"name\":\"customer_id\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"customer_1\",\"customer_2\",\"customer_3\",\"customer_4\",\"customer_5\",\"customer_6\",\"customer_7\",\"customer_8\",\"customer_9\",\"customer_10\"]}}},{\"name\":\"name\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Andi Wijaya\",\"Siti Rahma\",\"Budi Santoso\",\"Dewi Lestari\",\"Rizky Pratama\",\"Nadia Putri\",\"Fajar Nugroho\",\"Maya Sari\",\"Agus Setiawan\",\"Rina Marlina\"]}}},{\"name\":\"email\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"andi.wijaya@example.com\",\"siti.rahma@example.com\",\"budi.santoso@example.com\",\"dewi.lestari@example.com\",\"rizky.pratama@example.com\",\"nadia.putri@example.com\",\"fajar.nugroho@example.com\",\"maya.sari@example.com\",\"agus.setiawan@example.com\",\"rina.marlina@example.com\"]}}},{\"name\":\"gender\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Male\",\"Female\"]}}},{\"name\":\"region\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Jakarta\",\"Jawa Barat\",\"Jawa Tengah\",\"Jawa Timur\",\"Bali\",\"Sumatra Utara\",\"Sulawesi Selatan\"]}}},{\"name\":\"employment_type\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Employed\",\"Self-Employed\",\"Business Owner\",\"Student\",\"Retired\"]}}},{\"name\":\"dependents\",\"type\":{\"type\":\"int\",\"arg.properties\":{\"range\":{\"min\":0,\"max\":5}}}},{\"name\":\"credit_score\",\"type\":{\"type\":\"int\",\"arg.properties\":{\"range\":{\"min\":300,\"max\":850}}}},{\"name\":\"risk_tier\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Low\",\"Medium\",\"High\"]}}},{\"name\":\"account_tenure_years\",\"type\":{\"type\":\"int\",\"arg.properties\":{\"range\":{\"min\":0,\"max\":20}}}},{\"name\":\"loyalty_tier\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Bronze\",\"Silver\",\"Gold\",\"Platinum\"]}}}]}"
  }
}
```

#### 2. Products Datagen payload (catalog reference)

```json
{
  "name": "DatagenSource_Products",
  "config": {
    "connector.class": "DatagenSource",
    "name": "DatagenSource_Products",
    "kafka.api.key": "{{cluster_api_key}}",
    "kafka.api.secret": "{{cluster_api_secret}}",
    "kafka.topic": "products",
    "output.data.format": "AVRO",
    "max.interval": "1000",
    "tasks.max": "1",
    "schema.string": "{\"type\":\"record\",\"name\":\"Product\",\"fields\":[{\"name\":\"item_id\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"item_1\",\"item_2\",\"item_3\",\"item_4\",\"item_5\",\"item_6\",\"item_7\",\"item_8\",\"item_9\",\"item_10\"]}}},{\"name\":\"product_name\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Wireless Earbuds\",\"Running Shoes\",\"Rice Cooker\",\"Sofa Set\",\"Facial Serum\",\"Motor Oil\",\"Smart TV\",\"Backpack\",\"Blender\",\"Air Fryer\"]}}},{\"name\":\"category\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Electronics\",\"Fashion\",\"Groceries\",\"Home & Living\",\"Beauty\",\"Automotive\"]}}},{\"name\":\"price\",\"type\":{\"type\":\"double\",\"arg.properties\":{\"range\":{\"min\":50000,\"max\":5000000}}}},{\"name\":\"margin_tier\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"Low\",\"Medium\",\"High\"]}}},{\"name\":\"inventory_level\",\"type\":{\"type\":\"int\",\"arg.properties\":{\"range\":{\"min\":0,\"max\":1000}}}}]}"
  }
}
```

#### 3. Transactions Datagen payload (live orders)

```json
{
  "name": "DatagenSource_Transactions",
  "config": {
    "connector.class": "DatagenSource",
    "name": "DatagenSource_Transactions",
    "kafka.api.key": "{{cluster_api_key}}",
    "kafka.api.secret": "{{cluster_api_secret}}",
    "kafka.topic": "transactions",
    "output.data.format": "AVRO",
    "max.interval": "1000",
    "tasks.max": "1",
    "schema.string": "{\"type\":\"record\",\"name\":\"Transaction\",\"fields\":[{\"name\":\"order_id\",\"type\":{\"type\":\"long\",\"arg.properties\":{\"range\":{\"min\":1,\"max\":999999}}}},{\"name\":\"customer_id\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"customer_1\",\"customer_2\",\"customer_3\",\"customer_4\",\"customer_5\",\"customer_6\",\"customer_7\",\"customer_8\",\"customer_9\",\"customer_10\"]}}},{\"name\":\"item_id\",\"type\":{\"type\":\"string\",\"arg.properties\":{\"options\":[\"item_1\",\"item_2\",\"item_3\",\"item_4\",\"item_5\",\"item_6\",\"item_7\",\"item_8\",\"item_9\",\"item_10\"]}}},{\"name\":\"order_units\",\"type\":{\"type\":\"int\",\"arg.properties\":{\"range\":{\"min\":1,\"max\":5}}}},{\"name\":\"order_amount\",\"type\":{\"type\":\"double\",\"arg.properties\":{\"range\":{\"min\":10000,\"max\":5000000}}}}]}"
  }
}
```

---

## Step 3: Configure the Amazon Bedrock Connection and Model

Flink needs a connection to Amazon Bedrock and a registered model before it can call Claude from SQL.

1. In the AWS Console, open **Amazon Bedrock**.
<img width="2560" height="1223" alt="image" src="https://github.com/user-attachments/assets/81860c71-1e81-4a17-b9d6-219f0abcdde7" />

2. Click **Inference profiles** in the left-hand menu.
<img width="2560" height="1225" alt="image" src="https://github.com/user-attachments/assets/80bb5389-eb86-4810-a13d-d2066df94085" />

3. Copy the **inference profile ID** of your preferred Claude model (for example, a Claude 3.5 Haiku profile).
<img width="2560" height="1225" alt="image" src="https://github.com/user-attachments/assets/11be2987-c72f-483b-b91a-c1ae5708a0ce" />

4. Note the **Inference Profile ID**, for example:

```text
apac.anthropic.claude-3-haiku-20240307-v1:0
```

Now open the Confluent Cloud console. Click your environment name at the top left:
<img width="1644" height="787" alt="Picture12" src="https://github.com/user-attachments/assets/ecd53433-36a5-49ab-842f-3e6065125bd5" />

Click **Flink** in the left-hand menu:
<img width="2560" height="1230" alt="image" src="https://github.com/user-attachments/assets/508b92b1-3252-4f95-8097-4c82e568ca2c" />

Click the **Compute pools** tab: 
<img width="2560" height="1228" alt="image" src="https://github.com/user-attachments/assets/032ef4d5-6a51-4249-9dd1-c00de2d20283" />

Click **SQL Workspace** on the Flink compute pool you created earlier:
<img width="2560" height="1230" alt="image" src="https://github.com/user-attachments/assets/36df0728-d030-4962-aaa9-31d735464799" />

Run these statements in the Flink SQL workspace to create the connection:

```sql
CREATE CONNECTION bedrock_claude_connection
WITH (
  'type' = 'bedrock',
  'endpoint' = 'https://bedrock-runtime.<YOUR_AWS_REGION>.amazonaws.com/model/<INFERENCE_PROFILES_ID>/invoke',
  'aws-access-key' = '<YOUR_AWS_ACCESS_KEY>',
  'aws-secret-key' = '<YOUR_AWS_SECRET_KEY>'
);
```

**Example** (with a concrete region and inference profile):
```sql
CREATE CONNECTION bedrock_claude_connection
WITH (
  'type' = 'bedrock',
  'endpoint' = 'https://bedrock-runtime.ap-southeast-1.amazonaws.com/model/apac.anthropic.claude-3-haiku-20240307-v1:0/invoke',
  'aws-access-key' = 'AKIAIOSFODNN7EXAMPLE',
  'aws-secret-key' = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
);
```

> **Keep the model ID future-proof**  
> Replace `<INFERENCE_PROFILES_ID>` with whichever Claude Haiku-class model ID is currently enabled for your account on the Bedrock **Model access** page. This keeps the workshop working as Anthropic ships new model versions.

Finally, run this query to create the model:

```sql
CREATE MODEL claude_haiku_model
INPUT (text STRING)
OUTPUT (response STRING)
WITH (
  'provider' = 'bedrock',
  'task' = 'text_generation',
  'bedrock.input_format' = 'ANTHROPIC-MESSAGES',
  'bedrock.connection' = 'bedrock_claude_connection',
  'bedrock.params.max_tokens' = '1024'
);
```

---

## Step 4: Stream Processing - Enrichment and KYC Business Logic

### 4.1 Enrich transactions with the KYC profile and product catalog

This view joins each live transaction to the customer's KYC profile and to the product catalog, producing one wide, enriched row per order.

```sql
CREATE VIEW enriched_customer_orders AS
SELECT /*+ STATE_TTL('t' = '1h', 'c' = '1h', 'p' = '1h') */
  t.customer_id,
  t.order_id,
  t.item_id,
  t.order_units,
  t.order_amount,
  c.name,
  c.email,
  c.gender,
  c.region,
  c.employment_type,
  c.dependents,
  c.credit_score,
  c.risk_tier,
  c.account_tenure_years,
  c.loyalty_tier,
  p.product_name,
  p.category,
  p.price,
  p.margin_tier,
  p.inventory_level
FROM transactions t
JOIN customers c ON t.customer_id = c.customer_id
JOIN products p ON t.item_id = p.item_id;
```

> **Why the STATE_TTL hint**  
> The `STATE_TTL` hint caps how long Flink retains join state per input. This avoids the `HIGH_STATE_OPERATOR_WITHOUT_TTL` warning that Flink raises on an otherwise-unbounded three-way stream join.

### 4.2 Apply deterministic KYC business logic

This is the layer that decides what offer a customer qualifies for and whether it is worth a proactive email before any AI text generation happens.

```sql
CREATE VIEW nbo_business_logic AS
SELECT
  *,
  CASE
    WHEN employment_type = 'Business Owner' AND credit_score >= 650 THEN 'Business Line of Credit Offer'
    WHEN credit_score >= 750 AND risk_tier = 'Low' THEN 'Premium Financing Offer'
    WHEN risk_tier = 'High' OR credit_score < 580 THEN 'Secured / Low-Risk Starter Offer'
    WHEN dependents >= 3 THEN 'Family Bundle Discount'
    WHEN category = 'Electronics' AND margin_tier = 'High' THEN 'Extended Warranty Upsell'
    WHEN loyalty_tier IN ('Gold', 'Platinum') AND account_tenure_years >= 5 THEN 'Loyalty Reward Upgrade'
    ELSE 'Standard Cross-Sell Offer'
  END AS target_offer_quadrant,
  CASE
    WHEN credit_score >= 750 AND order_amount > 1000000 THEN 'VIP High-Value Customer'
    WHEN risk_tier = 'High' THEN 'High-Risk Monitored Customer'
    WHEN account_tenure_years >= 5 THEN 'Loyal Long-Tenure Customer'
    ELSE 'Standard Customer'
  END AS customer_segment,
  CASE
    WHEN credit_score >= 750 AND order_amount > 1000000 THEN TRUE
    WHEN risk_tier = 'High' THEN TRUE
    WHEN employment_type = 'Business Owner' AND credit_score >= 650 THEN TRUE
    ELSE FALSE
  END AS should_email
FROM enriched_customer_orders;
```

> **Why this separation matters**  
> Notice that `should_email` is fully deterministic and auditable, the AI never decides whether to reach out, only how to word the message once the business has already decided to. This clean split between deterministic business logic and generative wording is exactly what a regulated bank needs for compliance.

---

## Step 5: Agent 1 - NBO Recommender (Flink Streaming Agent)

This agent takes the deterministic quadrant and KYC context and turns it into a short, culturally appropriate promotional message.

```sql
CREATE AGENT nbo_recommender_agent
USING MODEL claude_haiku_model
USING PROMPT 'You are a retail banking Next-Best-Offer specialist.
You will be given a customer''s segment, region, purchase category, the
deterministic offer quadrant they qualify for, and their credit profile.
Write exactly one short, 1-2 sentence personalized promotional message that
presents the offer named in the "Recommended offer type" field.
Never invent a financial product outside of that recommended offer type.
Ensure the tone, references, and phrasing are culturally appropriate for the
customer''s specific region in Indonesia.'
WITH (
  'max_iterations' = '10',
  'tokens_management_strategy' = 'summarize',
  'max_tokens_threshold' = '100000',
  'summarization_prompt' = 'concise'
);
```

### Output table: next_best_offers

This table is the handoff point to Agent 2, so its key must be **raw bytes** (not Avro). This is a strict requirement for correct downstream upsert deduplication.

```sql
CREATE TABLE next_best_offers (
    `key` VARCHAR,
    customer_id VARCHAR,
    order_id BIGINT,
    customer_segment VARCHAR,
    target_offer_quadrant VARCHAR,
    category_purchased VARCHAR,
    region VARCHAR,
    email VARCHAR,
    should_email BOOLEAN,
    ai_generated_offer VARCHAR,
    PRIMARY KEY (`key`) NOT ENFORCED
) DISTRIBUTED BY HASH(`key`) INTO 1 BUCKETS
WITH (
    'connector' = 'confluent',
    'key.format' = 'raw',
    'value.format' = 'avro-registry'
);
```

> **Set the topic to compact after creating the table**  
> After running the statement above, go to **Topics → next_best_offers → Configuration** in the Confluent Cloud console and set `cleanup.policy` to `compact` (in addition to `delete`). This is required for correct last-write-wins behavior later.

### Run Agent 1 and write results

Use `INSERT INTO` execution (not `CREATE VIEW`, which does not support `LATERAL TABLE` in the console workspace), wrapped with a `GROUP BY` + `LAST_VALUE` reduction so Flink correctly derives `key` as the upsert key of the query:

```sql
INSERT INTO next_best_offers
SELECT
    `key`,
    LAST_VALUE(customer_id) AS customer_id,
    LAST_VALUE(order_id) AS order_id,
    LAST_VALUE(customer_segment) AS customer_segment,
    LAST_VALUE(target_offer_quadrant) AS target_offer_quadrant,
    LAST_VALUE(category) AS category_purchased,
    LAST_VALUE(region) AS region,
    LAST_VALUE(email) AS email,
    LAST_VALUE(should_email) AS should_email,
    LAST_VALUE(ai_generated_offer) AS ai_generated_offer
FROM (
    SELECT
        b.customer_id AS `key`,
        b.customer_id, b.order_id, b.customer_segment, b.target_offer_quadrant,
        b.category, b.region, b.email, b.should_email,
        a.response AS ai_generated_offer
    FROM nbo_business_logic b,
    LATERAL TABLE(
        AI_RUN_AGENT(
            'nbo_recommender_agent',
            'Customer segment: ' || b.customer_segment ||
            '. Region: ' || b.region || ', Indonesia' ||
            '. Category purchased: ' || b.category ||
            '. Recommended offer type: ' || b.target_offer_quadrant ||
            '. Credit score: ' || CAST(b.credit_score AS STRING) ||
            '. Risk tier: ' || b.risk_tier ||
            '. Write the personalized offer message now.',
            b.customer_id
        )
    ) AS a(status, response)
) raw_offers
GROUP BY `key`;
```

Since we only have 10 distinct customers in this workshop, this join and aggregation state stays naturally small.

---

## Step 6: Provision the Email Dispatch Tool (MCP Server)

This is the tool Agent 2 will call. It exposes a single `send_email` capability over the Model Context Protocol (MCP). In this workshop we use **Zapier's hosted MCP server** wired to the Gmail **Send Email** action.

### 1. Create a free Zapier account

Sign up for a free account at `zapier.com` and verify your email:
<img width="1588" height="757" alt="Picture1" src="https://github.com/user-attachments/assets/461104c4-5cb3-4f50-9bd2-f966852cd4c3" />

You will be asked a few onboarding questions (role, company size, seniority, relevant apps), answer these any way you like.
<img width="1294" height="614" alt="Picture2" src="https://github.com/user-attachments/assets/3f58cc0d-c5c8-4028-bcf2-41416a8efacc" />

<img width="1421" height="680" alt="Picture3" src="https://github.com/user-attachments/assets/06bc81cf-cabf-421f-a643-09f9f23d9d5f" />


### 2. Create the MCP server
Click **MCP Servers** in the left-hand menu:
<img width="1280" height="613" alt="2" src="https://github.com/user-attachments/assets/4fc0ab79-b66e-4414-9081-23c15c784f51" />

Click **See all**:
<img width="1251" height="601" alt="3" src="https://github.com/user-attachments/assets/e8b68af4-3f2f-4fec-a44e-0b44dd5aaccc" />

Click **Other**:
<img width="1295" height="620" alt="4" src="https://github.com/user-attachments/assets/0ac16778-cd0d-4918-b9d2-54c7ffc6ea86" />

Click **Add apps**: 
<img width="1311" height="612" alt="5" src="https://github.com/user-attachments/assets/a3ff45b9-a1b4-4391-a4d6-d997d60dfea7" />

Click **Gmail**: 
<img width="1318" height="631" alt="6" src="https://github.com/user-attachments/assets/9fe43d69-7d69-4782-852e-65b5b1f547af" />

Choose **Send Email** tool:
<img width="1297" height="623" alt="1" src="https://github.com/user-attachments/assets/a221d4e5-61a4-49d2-a403-4b7ac579cded" />

Click **Connect**:
<img width="1298" height="624" alt="2" src="https://github.com/user-attachments/assets/ce905820-fc01-4a50-a872-0ff20840dcaa" />

Work through the Gmail connection prompts by clicking Connect:
<img width="1316" height="630" alt="image" src="https://github.com/user-attachments/assets/9b473924-55a9-4d3c-88e0-e9c256e6046d" />

Choose your Google account: 
<img width="1305" height="625" alt="image" src="https://github.com/user-attachments/assets/49d1bfc4-bf96-478d-a79d-360d3f92e7b1" />

Click **Continue**: 
<img width="1302" height="623" alt="image" src="https://github.com/user-attachments/assets/8b61d066-d26c-40e5-a41d-d39deeaf561b" />

Grant Zapier the required permissions by checking all the boxes: 
<img width="1306" height="625" alt="image" src="https://github.com/user-attachments/assets/5ee16e30-d7a9-411e-8da3-7ec1aaf976d4" />

Scroll down and click **Continue** 
<img width="1319" height="631" alt="image" src="https://github.com/user-attachments/assets/740237c6-5946-4576-9371-889e56f268b3" />

Click **Add tool**.
<img width="1321" height="629" alt="image" src="https://github.com/user-attachments/assets/07a46c57-5f33-4ba5-bbb5-20a3da15aa12" />

Open the **Connect** tab: 
<img width="1314" height="632" alt="image" src="https://github.com/user-attachments/assets/0271c96c-08ba-4a13-939a-23ecac2bb311" />

Click **Generate token**. 
<img width="1314" height="629" alt="image" src="https://github.com/user-attachments/assets/811b1690-3a85-470e-9da5-a0deda13dd21" />

Copy and save the token. You will paste it into the Flink MCP connection in Step 7.

> **Guard your Zapier token**  
> The generated token authorizes sending email from your connected Gmail account. Treat it like a password: do not commit it to a repository, and do not leave it visible on a projected screen.

---

## Step 7: Agent 2 - Email Dispatch Agent (Flink Streaming Agent)

### 7.1 Connect Flink to the email MCP server

Create the MCP connection, substituting your environment, cluster, and Zapier token:

```sql
CREATE CONNECTION `<environment>`.`<kafka_cluster>`.`mcp_connection`
WITH (
  'type' = 'mcp_server',
  'endpoint' = 'https://mcp.zapier.com/api/v1/connect',
  'token' = '<your_zapier_token>',
  'transport-type' = 'STREAMABLE_HTTP'
);
```

**Example:**

```sql
CREATE CONNECTION `workshop-kyc-nbo`.`kyc-nbo-cluster`.`email_mcp_connection`
WITH (
  'type' = 'mcp_server',
  'endpoint' = 'https://mcp.zapier.com/api/v1/connect',
  'token' = 'REPLACE_WITH_YOUR_ZAPIER_MCP_TOKEN',
  'transport-type' = 'STREAMABLE_HTTP'
);
```

Then register the `send_email` tool against that connection:

```sql
CREATE TOOL send_email_tool
USING CONNECTION email_mcp_connection
WITH (
  'type' = 'mcp',
  'allowed_tools' = 'send_email',
  'request_timeout' = '30'
);
```

### 7.2 Create the agent

Agent 2 composes a subject and body from the approved offer and calls the `send_email` tool exactly once.

```sql
CREATE AGENT email_dispatch_agent
USING MODEL claude_haiku_model
USING PROMPT 'You are an email composer for a retail bank''s marketing team.
You will be given a customer''s email address and a short offer message that has
already been approved for outreach.
Write a polished subject line and a friendly, professional email body that
incorporates the offer message, then call the send_email tool exactly once.
The "to" parameter must be passed as a single plain string, never an array.'
USING TOOLS send_email_tool
WITH (
  'max_iterations' = '10',
  'max_consecutive_failures' = '3',
  'tokens_management_strategy' = 'summarize',
  'max_tokens_threshold' = '100000',
  'summarization_prompt' = 'concise'
);
```

### 7.3 Trigger Agent 2 from Agent 1's output - the agent-to-agent handoff

Agent 1 and Agent 2 never call each other as functions. Agent 2 simply reacts to rows landing in `next_best_offers` where `should_email = TRUE`, the Kafka topic itself is the communication channel between the two agents. We use `INSERT INTO` (not `CREATE VIEW`), since `LATERAL TABLE` inside a view is unreliable in the console workspace:

```sql
CREATE TABLE email_dispatch_log (
  dispatch_time TIMESTAMP_LTZ(3),
  customer_id VARCHAR,
  status VARCHAR,
  response VARCHAR
);
```

```sql
ALTER TABLE next_best_offers SET ('changelog.mode' = 'append');
```

```sql
INSERT INTO email_dispatch_log
SELECT
  CURRENT_TIMESTAMP,
  n.customer_id,
  a.status, a.response
FROM (
  SELECT
    customer_id,
    order_id,
    email,
    ai_generated_offer
  FROM next_best_offers
  WHERE should_email = TRUE
) AS n,
LATERAL TABLE(
  AI_RUN_AGENT(
    'email_dispatch_agent',
    CONCAT(
      'To: ',
      SUBSTRING(COALESCE(n.email, ''), 1, 320),
      '. Offer: ',
      SUBSTRING(COALESCE(n.ai_generated_offer, ''), 1, 500),
      '. Send now.'
    ),
    CONCAT(
      CAST(n.customer_id AS STRING),
      '-',
      CAST(n.order_id AS STRING)
    )
  )
) AS a(status, response);
```

> **What you should see**  
> Once this runs, check your email dispatch destination, the Gmail Sent folder (or your Zapier task history), and `email_dispatch_log` in Flink. You should see personalized emails appearing for every qualifying customer, composed entirely by Agent 2 and dispatched entirely because Agent 1 decided to hand the work off. (If you chose the local Python mock server instead, watch its terminal and the `sent_emails.log` file grow.)

---

## Step 8: End-to-End Test

1. Confirm all three Datagen connectors are **RUNNING** and producing to `customers`, `products`, and `transactions`.
2. Confirm `enriched_customer_orders` and `nbo_business_logic` return rows with a simple query: `SELECT * FROM nbo_business_logic LIMIT 10;`
3. Confirm `next_best_offers` is populating: `SELECT * FROM next_best_offers;`
4. Confirm `email_dispatch_log` is populating for `should_email = TRUE` rows, and that emails are actually going out, check the connected Gmail account's Sent folder and your Zapier task history (or, for the local mock, confirm `sent_emails.log` is growing).

---

## Expected Results

- A live, three-source, KYC-aware Next Best Offer pipeline running entirely on Confluent Cloud and Flink SQL.
- A clear separation between deterministic, auditable business logic (who qualifies, for what, and whether to email) and generative AI (how to word it), the standard pattern for AI in regulated industries.
- Two independently deployed, independently scalable Flink Streaming Agents that hand off work purely through a Kafka topic, with every decision and tool call logged and replayable.
- A working email dispatch you can watch happen live, end to end, driven entirely from inside Confluent Cloud.

---
