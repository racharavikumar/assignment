"""Learning Wise web application."""

__version__ = "0.1.0"


def create_app():
    """Create and configure the Flask application."""
    from flask import Flask
    
    app = Flask(__name__)
    
    @app.route('/')
    def index():
        return {'message': 'Learning Wise API', 'status': 'running'}
    
    @app.route('/api/sum/<int:n>')
    def calculate_sum(n):
        """Calculate sum of numbers from 1 to n."""
        total = sum(range(1, n + 1))
        return {'n': n, 'sum': total}
    
    return app


if __name__ == '__main__':
    app = create_app()
    app.run(debug=True)
