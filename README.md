# Python Clients for CleverMaps Data Operations

A Python client library providing high-level client interfaces for common CleverMaps operations.

## Overview

This package simplifies interaction with CleverMaps projects by providing intuitive, high-level methods that handle complex workflows behind the scenes. It's built on top of the CleverMaps Python OpenAPI SDK.
## Features

- **Data Dumping**: Export datasets from CleverMaps projects to CSV files
- **Data Loading**: Upload CSV files to CleverMaps projects:
  - Single-part uploads for small files
  - Multipart uploads with GZIP compression for large files

## Installation

```bash
pip install git+https://github.com/clevermaps/cm-python-client.git
```

Then import the package:
```python
import cm_python_clients
```

## Example usage

```python
from cm_python_clients import DataDumpClient

access_token = "<your access token>"
project_id = "<your project id>"
dataset = "<your dataset name>"
output_path = "<your output path>"

sdk = DataDumpClient(api_token=access_token)

output_file = sdk.dump_dataset_to_csv(project_id, dataset, output_path)
```

