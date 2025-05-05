# LogParser2Clickhouse

This project is designed to facilitate collaboration and communication among Wipro partners. It provides a platform for sharing resources, discussions, and updates.

## Features
- Partner collaboration tools
- Resource sharing
- Discussion forums
- Notifications and updates

## Prerequisites
Before setting up the project, ensure you have the following installed:
- [Python](https://www.python.org/) (v3.8 or higher)
- [pip](https://pip.pypa.io/en/stable/)
- [Git](https://git-scm.com/)

## Setup Instructions

1. Create and activate a virtual environment:
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Configure environment variables:
    - Create a `.env` file in the root directory.
    - Add the required environment variables (e.g., database connection strings, API keys). Refer to `.env.example` for guidance.


4. Start the development server:
    ```bash
    python run.py
    ```


## Changes Needed to Run the Project
- Update the `.env` file with your specific configuration (e.g., database credentials, API keys).
- Ensure the backend services (if any) are running and accessible.
- Modify any hardcoded URLs or paths in the codebase to match your environment.