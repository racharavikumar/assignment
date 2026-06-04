pipeline {
  agent any

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Build Docker Image') {
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

    stage('Run sample_script.py') {
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

    stage('Run Tests') {
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

    stage('Build Package') {
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
  }

  post {
    always {
      archiveArtifacts artifacts: 'dist/**, **/build/**/*.zip', allowEmptyArchive: true
      cleanWs()
    }
  }
}


