#!/usr/bin/env python

import os
import sys
import logging

# Set up logging configuration
logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)


def main():
    # Add the project folder to the Python path so that imports work
    # sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'WiproPartnersForum')))
    sys.path.append(os.path.abspath(os.path.dirname(__file__)))
    
    # Import your app.py logic here
    from app.app import main  # assuming your app.py has a `main()` function
    
    try:
        # Run the main function from app.py
        logging.info("Application started...")
        main()
    except Exception as e:
        logging.error(f"Error starting application: {e}", exc_info=True)

if __name__ == '__main__':
    main()
