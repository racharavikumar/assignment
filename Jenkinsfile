pipeline {
  agent any

  environment {
    PYTHON = 'python'
    VENV_DIR = '.venv'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Setup Environment') {
      steps {
        catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {
          script {
            if (isUnix()) {
              sh "${PYTHON} -m venv ${VENV_DIR}"
              sh ". ${VENV_DIR}/bin/activate && pip install --upgrade pip"
            } else {
              bat "${PYTHON} -m venv ${VENV_DIR}"
              bat "${VENV_DIR}\\Scripts\\pip.exe install --upgrade pip"
            }
          }
        }
      }
    }

    stage('Install Dependencies') {
      steps {
        catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {
          script {
            if (isUnix()) {
              sh ". ${VENV_DIR}/bin/activate && pip install -r requirements.txt"
            } else {
              bat "${VENV_DIR}\\Scripts\\pip.exe install -r requirements.txt"
            }
          }
        }
      }
    }

    stage('Install Project') {
      steps {
        catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {
          script {
            if (isUnix()) {
              sh ". ${VENV_DIR}/bin/activate && python -m pip install -e ."
            } else {
              bat "${VENV_DIR}\\Scripts\\python.exe -m pip install -e ."
            }
          }
        }
      }
    }

    stage('Run sample_script.py') {
      steps {
        script {
          if (isUnix()) {
            sh ". ${VENV_DIR}/bin/activate && python sample_script.py"
          } else {
            bat "${VENV_DIR}\\Scripts\\python.exe sample_script.py"
          }
        }
      }
    }

    stage('Run Tests') {
      steps {
        script {
          if (isUnix()) {
            sh ". ${VENV_DIR}/bin/activate && python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml"
          } else {
            bat "${VENV_DIR}\\Scripts\\python.exe -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml"
          }
        }
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'tests/junit-results.xml'
          publishHTML([
            reportDir: 'htmlcov',
            reportFiles: 'index.html',
            reportName: 'Coverage Report',
            keepAll: true
          ])
        }
      }
    }

    stage('Build Package') {
      steps {
        catchError(buildResult: 'SUCCESS', stageResult: 'UNSTABLE') {
          script {
            if (isUnix()) {
              sh ". ${VENV_DIR}/bin/activate && python -m pip install --upgrade build"
              sh ". ${VENV_DIR}/bin/activate && python -m build --wheel --outdir dist"
            } else {
              bat "${VENV_DIR}\\Scripts\\python.exe -m pip install --upgrade build"
              bat "${VENV_DIR}\\Scripts\\python.exe -m build --wheel --outdir dist"
            }
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


