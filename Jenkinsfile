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
            if (fileExists('requirements.txt')) {
              if (isUnix()) {
                sh ". ${VENV_DIR}/bin/activate && pip install -r requirements.txt"
              } else {
                bat "${VENV_DIR}\\Scripts\\pip.exe install -r requirements.txt"
              }
            } else {
              echo 'No requirements.txt found; skipping dependency install.'
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
              sh ". ${VENV_DIR}/bin/activate && python -m pip install -e python_project"
            } else {
              bat "${VENV_DIR}\\Scripts\\python.exe -m pip install -e python_project"
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
            sh ". ${VENV_DIR}/bin/activate && python -m pytest -q"
          } else {
            bat "${VENV_DIR}\\Scripts\\python.exe -m pytest -q"
          }
        }
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'tests/**/*.xml'
        }
      }
    }

    stage('Build Package') {
      steps {
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

  post {
    always {
      archiveArtifacts artifacts: 'dist/**, **/build/**/*.zip', allowEmptyArchive: true
      cleanWs()
    }
  }
}


