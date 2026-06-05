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

    stage('Detect Docker') {
      steps {
        script {
          def dockerAvailable = false
          if (isUnix()) {
            try {
              sh 'docker --version'
              dockerAvailable = true
              echo 'Docker available on Unix agent.'
            } catch (err) {
              echo 'Docker not available on Unix agent.'
            }
          } else {
            try {
              bat 'docker --version'
              dockerAvailable = true
              echo 'Docker available on Windows agent.'
            } catch (err) {
              echo 'Docker not available on Windows agent.'
            }
          }
          env.DOCKER_AVAILABLE = dockerAvailable ? 'true' : 'false'
        }
      }
    }

    stage('Setup Environment') {
      steps {
        script {
          if (env.DOCKER_AVAILABLE == 'true') {
            echo 'Skipping venv setup, Docker will be used.'
          } else {
            echo 'Setting up native Python venv...'
            if (isUnix()) {
              sh '''
                python -m venv .venv
                . .venv/bin/activate && pip install --upgrade pip
                . .venv/bin/activate && pip install -r requirements.txt
                . .venv/bin/activate && python -m pip install -e .
              '''
            } else {
              bat "python -m venv .venv"
              bat ".venv\\Scripts\\pip.exe install --upgrade pip"
              bat ".venv\\Scripts\\pip.exe install -r requirements.txt"
              bat ".venv\\Scripts\\python.exe -m pip install -e ."
            }
          }
        }
      }
    }

    stage('Build Image or Package') {
      steps {
        script {
          if (env.DOCKER_AVAILABLE == 'true') {
            if (isUnix()) {
              sh "docker build -t learning-wise:${BUILD_NUMBER} ."
            } else {
              bat "docker build -t learning-wise:%BUILD_NUMBER% ."
            }
          } else {
            if (isUnix()) {
              sh ". .venv/bin/activate && python -m pip install --upgrade build && python -m build --wheel --outdir dist"
            } else {
              bat ".venv\\Scripts\\python.exe -m pip install --upgrade build"
              bat ".venv\\Scripts\\python.exe -m build --wheel --outdir dist"
            }
          }
        }
      }
    }

    stage('Run sample_script.py') {
      steps {
        script {
          if (env.DOCKER_AVAILABLE == 'true') {
            if (isUnix()) {
              sh "docker run --rm -v ${WORKSPACE}:/app learning-wise:${BUILD_NUMBER} python sample_script.py"
            } else {
              bat "docker run --rm -v %WORKSPACE%:/app learning-wise:%BUILD_NUMBER% python sample_script.py"
            }
          } else {
            if (isUnix()) {
              sh ". .venv/bin/activate && python sample_script.py"
            } else {
              bat ".venv\\Scripts\\python.exe sample_script.py"
            }
          }
        }
      }
    }

    stage('Run Tests') {
      steps {
        script {
          if (env.DOCKER_AVAILABLE == 'true') {
            if (isUnix()) {
              sh "docker run --rm -v ${WORKSPACE}:/app learning-wise:${BUILD_NUMBER} python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml"
            } else {
              bat "docker run --rm -v %WORKSPACE%:/app learning-wise:%BUILD_NUMBER% python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml"
            }
          } else {
            if (isUnix()) {
              sh ". .venv/bin/activate && python -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml"
            } else {
              bat ".venv\\Scripts\\python.exe -m pytest -q --junit-xml=tests/junit-results.xml --cov=learning_wise --cov-report=html --cov-report=xml"
            }
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
  }

  post {
    always {
      archiveArtifacts artifacts: 'dist/**, **/build/**/*.zip', allowEmptyArchive: true
      cleanWs()
    }
  }
}
