# CloudBox 2.0 — Intelligent Distributed Cloud Storage

CloudBox is an intelligent distributed cloud storage system built for Cloud & Distributed Computing. It features AES encryption at rest, automatic chunking, multi-node replication, load-aware placement, automated self-healing, real-time performance analytics, and concurrent parallel transfers.

## Architecture & Features

### 1. Storage Reliability & Consistency
- **Transactional Uploads**: Multi-stage upload transactions mark files as `"uploading"` until all chunks and replicas are verified, preventing incomplete files from appearing in file listings.
- **Orphan Cleanup**: Partial upload failures immediately purge all allocated chunks from disk. Stalled uploads from system crashes are purged automatically on service startup.
- **Verified Repairs**: Re-replication verifies Fernet decryption and SHA-256 checksums before copying to prevent propagating silent bit rot or corrupted data.
- **Pending Deletes Queue**: Files deleted while a storage node is offline are tracked in `pending_deletes` metadata and cleanly pruned when the node comes back online.
- **Metadata Concurrency Locks**: Thread-safe operations protect metadata consistency under high-concurrency workloads.

### 2. Intelligent Load Balancing
- **Load Scoring**: Dynamic load scoring evaluates stored bytes (70%), chunk counts (30%), and node availability.
- **Load-Aware Placement**: Directs new chunks toward less loaded nodes to balance storage skew without ever placing duplicate replicas on the same node.
- **Mid-Upload Failure Handling**: Dynamically queries alive nodes and adapts placement if a node fails mid-transfer.
- **Observability**: Placement strategy decisions (`"load-aware"` or `"round-robin"`) are recorded per chunk in metadata.
- **Interactive Skew Benchmark**: Built-in benchmark simulates skewed workloads and demonstrates lower variance with load-aware placement.

### 3. Automatic Failure Detection & Self-Healing
- **Node Health Endpoints**: Each node exposes `/health` with status, chunk counts, and capacity.
- **Background Health Loop**: Runs every 10 seconds to detect missing replicas, unreachable nodes, and checksum mismatches.
- **Three-Tier Health State**: Maintains real-time status for every file:
  - **Healthy**: All replicas intact and verified.
  - **Degraded**: Fewer replicas than required, but file remains recoverable.
  - **At Risk**: Only one replica remaining or corruption detected.
- **Automated Self-Healing**: Automatically restores replication count onto the least-loaded healthy nodes without manual intervention.
- **Concurrency Protection**: Non-blocking lock prevents duplicate or overlapping repair jobs.
- **Recovery Event Audit Logging**: Logs source node, target node, chunk ID, timestamp, and repair type.

### 4. Performance & Storage Analytics Dashboard
- **SQLite Metrics Persistence**: Operation metrics are recorded in `metrics.db` across transfers, recoveries, and system snapshots.
- **Throughput & Duration Tracking**: Computes upload and download transfer latency (ms) and throughput (MB/s).
- **Efficiency Metrics**: Tracks real-time storage consumption per node and system-level replication overhead.
- **Interactive Chart.js Visualizations**:
  - Real-time line chart for throughput over time.
  - Real-time bar chart for storage utilization per node.
  - System health cards and replication overhead counter.
- **CSV Data Export**: Download performance and transfer history via `/api/metrics/export`.

### 5. Concurrent Parallel Chunk Transfers
- **ThreadPoolExecutor Concurrency**: Uploads and downloads chunks concurrently across storage nodes (configurable concurrency, default 4 workers).
- **Bounded Retries with Backoff**: Automatically retries transient network errors up to 3 attempts with exponential backoff.
- **Strict Chunk Ordering**: Downloads reassemble chunks in exact original sequence regardless of thread completion order.
- **HTTP Connection Pooling**: Thread-safe HTTP connection pooling via `requests.adapters.HTTPAdapter`.
- **Transfer Speed Benchmark**: Interactive benchmark comparing sequential vs. parallel transfer speeds with bit-for-bit file verification.

---

## Getting Started

### 1. Install & Run

```bash
pip install -r requirements.txt
python app.py
```

Open **http://localhost:8000** in your browser.

### 2. Running Tests

The test suite validates all features across all 5 upgrade phases:

```bash
python -m unittest test_cloudbox.py -v
```

Ran **34 tests** with 100% pass rate.

---

## System Components

| File | Role |
|---|---|
| `node.py` | Storage node HTTP server (`/chunks`, `/stats`, `/health`) on ports 8001–8003 |
| `storage.py` | Core distributed storage engine (chunking, encryption, replication, load balancing, self-healing, parallel transfers) |
| `metrics.py` | SQLite performance and storage analytics persistence (`metrics.db`) |
| `app.py` | Flask API gateway, background monitors, health and metrics endpoints |
| `static/index.html` | Real-time web dashboard with Chart.js charts and benchmark modals |
| `test_cloudbox.py` | 34 automated unit and integration tests |
| `render.yaml` | 1-Click Render blueprint configuration |
| `Dockerfile` | Container configuration for Koyeb, Hugging Face Spaces, and Docker hosts |
| `Procfile` | WSGI start command for cloud buildpacks |

---

## Free Cloud Deployment (Zero Credit Card Required)

CloudBox includes ready-to-use configuration files for top free cloud hosting providers that do not require any credit card:

### Option 1: Render (Recommended — Easiest & Fastest)
- **Sign Up**: Go to [render.com](https://render.com) and sign in using your GitHub account (No credit card required).
- **Deploy**:
  1. Click **New +** -> **Web Service**.
  2. Select your repository `mirliaqat/cloudbox`.
  3. Render will automatically detect the settings:
     - **Runtime**: `Python`
     - **Build Command**: `pip install -r requirements.txt`
     - **Start Command**: `gunicorn --workers 1 --threads 8 --bind 0.0.0.0:$PORT app:app`
     - **Instance Type**: `Free`
  4. Click **Deploy Web Service**.
  5. Your app will be live at `https://<service-name>.onrender.com` in ~2 minutes with automatic HTTPS!

### Option 2: Koyeb
- **Sign Up**: Go to [koyeb.com](https://www.koyeb.com) and log in with GitHub (No credit card needed for free hobby tier).
- **Deploy**:
  1. Click **Create App** -> **GitHub**.
  2. Select `mirliaqat/cloudbox`.
  3. Koyeb will automatically detect the `Dockerfile` in the repo.
  4. Select the **Nano (Free)** tier and click **Deploy**.

### Option 3: PythonAnywhere
- **Sign Up**: Create a free "Beginner" account at [pythonanywhere.com](https://www.pythonanywhere.com) (No credit card needed).
- **Deploy**:
  1. Open a **Bash Console** and run:
     ```bash
     git clone https://github.com/mirliaqat/cloudbox.git
     cd cloudbox
     pip install -r requirements.txt
     ```
  2. Go to the **Web** tab -> **Add a new web app** -> **Manual configuration** -> **Python 3.10+**.
  3. In the WSGI configuration file, point to `from app import app as application`.
  4. Reload the web app.

### Option 4: Hugging Face Spaces
- **Sign Up**: Sign in to [huggingface.co](https://huggingface.co) (Free, no credit card).
- **Deploy**:
  1. Click **New Space**.
  2. Select SDK: **Docker** (Blank).
  3. Clone the space repo or connect GitHub and push the CloudBox code. It will build and run on port 7860 automatically!

