pipeline {
  agent any

  environment {
    PYTHON = 'python'
    VENV_DIR = '.venv'
    DOCKER_AVAILABLE = 'false'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Detect Docker') {
      steps {
        script {
          if (isUnix()) {
            try {
              sh 'docker --version'
              env.DOCKER_AVAILABLE = 'true'
            } catch (err) {
              env.DOCKER_AVAILABLE = 'false'
              echo 'Docker not available on this agent; falling back to native Python execution.'
            }
          } else {
            try {
              bat 'docker --version'
              env.DOCKER_AVAILABLE = 'true'
            } catch (err) {
              env.DOCKER_AVAILABLE = 'false'
              echo 'Docker not available on this agent; falling back to native Python execution.'
            }
          }
        }
      }
    }

    stage('Build Docker Image') {
      when {
        expression { env.DOCKER_AVAILABLE == 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh 'docker build -t learning-wise:${BUILD_NUMBER} .'
          } else {
            bat 'docker build -t learning-wise:%BUILD_NUMBER% .'
          }
        }
      }
    }

    stage('Run sample_script.py (Docker)') {
      when {
        expression { env.DOCKER_AVAILABLE == 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh 'docker run --rm -v "${WORKSPACE}:/app" learning-wise:${BUILD_NUMBER} python sample_script.py'
          } else {
            bat 'docker run --rm -v "%WORKSPACE%:/app" learning-wise:%BUILD_NUMBER% python sample_script.py'
          }
        }
      }
    }

    stage('Run Tests (Docker)') {
      when {
        expression { env.DOCKER_AVAILABLE == 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh 'docker run --rm -v "${WORKSPACE}:/app" learning-wise:${BUILD_NUMBER} python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml'
          } else {
            bat 'docker run --rm -v "%WORKSPACE%:/app" learning-wise:%BUILD_NUMBER% python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml'
          }
        }
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'tests/junit-results.xml'
          archiveArtifacts artifacts: 'htmlcov/**, coverage.xml', allowEmptyArchive: true
        }
      }
    }

    stage('Build Package (Docker)') {
      when {
        expression { env.DOCKER_AVAILABLE == 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh 'docker run --rm -v "${WORKSPACE}:/app" learning-wise:${BUILD_NUMBER} python -m pip install --upgrade build && python -m build --wheel --outdir dist'
          } else {
            bat 'docker run --rm -v "%WORKSPACE%:/app" learning-wise:%BUILD_NUMBER% python -m pip install --upgrade build && python -m build --wheel --outdir dist'
          }
        }
      }
    }

    stage('Native Setup') {
      when {
        expression { env.DOCKER_AVAILABLE != 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh 'python -m venv .venv'
            sh '. .venv/bin/activate && pip install --upgrade pip'
            sh '. .venv/bin/activate && pip install -r requirements.txt'
            sh '. .venv/bin/activate && python -m pip install -e .'
          } else {
            bat 'python -m venv .venv'
            bat '.venv\\Scripts\\pip.exe install --upgrade pip'
            bat '.venv\\Scripts\\pip.exe install -r requirements.txt'
            bat '.venv\\Scripts\\python.exe -m pip install -e .'
          }
        }
      }
    }

    stage('Run sample_script.py (Native)') {
      when {
        expression { env.DOCKER_AVAILABLE != 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh '. .venv/bin/activate && python sample_script.py'
          } else {
            bat '.venv\\Scripts\\python.exe sample_script.py'
          }
        }
      }
    }

    stage('Run Tests (Native)') {
      when {
        expression { env.DOCKER_AVAILABLE != 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh '. .venv/bin/activate && python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml'
          } else {
            bat '.venv\\Scripts\\python.exe -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml'
          }
        }
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'tests/junit-results.xml'
          archiveArtifacts artifacts: 'htmlcov/**, coverage.xml', allowEmptyArchive: true
        }
      }
    }

    stage('Build Package (Native)') {
      when {
        expression { env.DOCKER_AVAILABLE != 'true' }
      }
      steps {
        script {
          if (isUnix()) {
            sh '. .venv/bin/activate && python -m pip install --upgrade build && python -m build --wheel --outdir dist'
          } else {
            bat '.venv\\Scripts\\python.exe -m pip install --upgrade build && .venv\\Scripts\\python.exe -m build --wheel --outdir dist'
          }
        }
      }
    }
  }

  post {
    always {
      archiveArtifacts artifacts: 'dist/**, **/build/**/*.zip', allowEmptyArchive: true
      cleanWs()
    }
  }
}


