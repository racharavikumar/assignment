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
          dockerAvailable = false
          if (isUnix()) {
            try {
              sh 'docker --version'
              dockerAvailable = true
            } catch (err) {
              echo 'Docker not available on Unix agent.'
            }
          } else {
            try {
              bat 'docker --version'
              dockerAvailable = true
            } catch (err) {
              echo 'Docker not available on Windows agent.'
            }
          }
          // Save result for later stages
          env.DOCKER_AVAILABLE = dockerAvailable ? 'true' : 'false'
        }
      }
    }

    stage('Build Docker Image') {
      when { expression { env.DOCKER_AVAILABLE == 'true' } }
      steps {
        script {
          if (isUnix()) {
            sh "docker build -t learning-wise:${BUILD_NUMBER} ."
          } else {
            bat "docker build -t learning-wise:%BUILD_NUMBER% ."
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
              sh "docker run --rm -v ${WORKSPACE}:/app learning-wise:${BUILD_NUMBER} python -m pytest -q --junit-xml=tests/junit-results.xml"
            } else {
              bat "docker run --rm -v %WORKSPACE%:/app learning-wise:%BUILD_NUMBER% python -m pytest -q --junit-xml=tests/junit-results.xml"
            }
          } else {
            if (isUnix()) {
              sh ". .venv/bin/activate && python -m pytest -q --junit-xml=tests/junit-results.xml"
            } else {
              bat ".venv\\Scripts\\python.exe -m pytest -q --junit-xml=tests/junit-results.xml"
            }
          }
        }
      }
      post {
        always {
          junit allowEmptyResults: true, testResults: 'tests/junit-results.xml'
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
