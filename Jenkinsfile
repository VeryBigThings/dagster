pipeline {
  agent any
  stages {
    stage('build image') {
      steps {
        sh "docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} ."
      }
    }

  }
  environment {
    DOCKER_IMAGE = 'dagster'
    DOCKER_TAG = 'latest'
  }
}
