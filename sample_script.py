"""Sample script to test the Learning Wise web application.

This script creates a Flask app, makes test requests to verify it works,
and writes a summary to output.txt.
Exit code 0 on success.
"""

import sys
import json
import io


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
            print("  Index: PASS - {}".format(data))
            
            # Test sum endpoint
            response = client.get('/api/sum/5')
            assert response.status_code == 200
            data = response.get_json()
            print("  Sum endpoint: PASS - {}".format(data))
            assert data['sum'] == 15
            
        print("All endpoint tests passed!")
        
        # Write summary to output.txt with explicit encoding
        output_content = """Learning Wise Web Application Test Results
==================================================
[PASS] App created successfully
[PASS] Index route working
[PASS] Sum API endpoint working
[PASS] All tests passed
"""
        
        with io.open("output.txt", "w", encoding="utf-8") as f:
            f.write(output_content)
        
        print("Results written to output.txt")
        return 0
        
    except Exception as e:
        print("Error: {}".format(str(e)))
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
