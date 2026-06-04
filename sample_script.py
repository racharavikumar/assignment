"""Sample script to test the Learning Wise web application.

This script creates a Flask app, makes test requests to verify it works,
and writes a summary to output.txt.
Exit code 0 on success.
"""

import sys
import json


def main():
    """Test the web application."""
    try:
        from learning_wise import create_app
        
        print("Creating Learning Wise Flask app...")
        app = create_app()
        
        print("Testing app endpoints...")
        with app.test_client() as client:
            # Test index route
            response = client.get('/')
            assert response.status_code == 200
            data = response.get_json()
            print(f"  Index: {data}")
            
            # Test sum endpoint
            response = client.get('/api/sum/5')
            assert response.status_code == 200
            data = response.get_json()
            print(f"  Sum endpoint: {data}")
            assert data['sum'] == 15
            
        print("✓ All endpoint tests passed!")
        
        # Write summary to output.txt
        with open("output.txt", "w", encoding="utf-8") as f:
            f.write("Learning Wise Web Application Test Results\n")
            f.write("=" * 50 + "\n")
            f.write("✓ App created successfully\n")
            f.write("✓ Index route working\n")
            f.write("✓ Sum API endpoint working\n")
            f.write("✓ All tests passed\n")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
