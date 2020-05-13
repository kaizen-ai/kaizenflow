#!groovy
import groovy.json.JsonOutput
import groovy.json.JsonSlurperClassic

// GitHub API

// Example:
// $ curl "${data_pull_request_statuses_url}?access_token=${P1_GITHUB_API_TOKEN}" \
//	-H "Content-Type: application/json" \
//	-X POST \
//    -d "${job_pending_payload}"
def postRequestGhApi(String url, Map payload) {
    // Perform POST request to GitHub API using given url and payload

    // Add token defined on Jenkins level to the url
    def tokenizedUrl = "${url}?access_token=${P1_GITHUB_API_TOKEN}"
    println("Tokenized URL: ${tokenizedUrl}")
    // Convert map to Json
    def message = JsonOutput.toJson(payload)
    println("Payload message=${message}")
    def post = new URL(tokenizedUrl).openConnection();
    post.setRequestMethod("POST")
    post.setDoOutput(true)
    post.setRequestProperty("Content-Type", "application/json")
    post.getOutputStream().write(message.getBytes("UTF-8"));
    def postRC = post.getResponseCode();
    println("Response code: ${postRC}")
    println("postRC type: ${postRC.getClass()}")
    if (postRC != 201) {
        println("Print content.")
        println("Responce error: ${post.getErrorStream()}")
    }
}

def deleteRequestGhApi(String url) {
    // Perform DELETE request to GitHub API using given url

    // Add token defined on Jenkins level to the url
    def tokenizedUrl = "${url}?access_token=${P1_GITHUB_API_TOKEN}"
    println("GitHub API url: ${tokenizedUrl}")
    def post = new URL(tokenizedUrl).openConnection();
    post.setRequestMethod("DELETE")
    post.setDoOutput(true)
    post.setRequestProperty("Content-Type", "application/json")
    def postRC = post.getResponseCode();
    println("Response code: ${postRC}")
    println("postRC type: ${postRC.getClass()}")
    if (postRC != 200) {
        println("Print content.")
        println("Responce error: ${post.getErrorStream()}")
    }
}

def getRequestGhApi(String url) {
    // Perform GET request to GitHub API using given url

    // Add token defined on Jenkins level to the url
    def tokenizedUrl = "${url}?access_token=${P1_GITHUB_API_TOKEN}"
    println("GitHub API url: ${tokenizedUrl}")
    def post = new URL(tokenizedUrl).openConnection();
    post.setRequestMethod("GET")
    post.setDoOutput(true)
    post.setRequestProperty("Content-Type", "application/json")
    def postRC = post.getResponseCode();
    println("Response code: ${postRC}")
    println("postRC type: ${postRC.getClass()}")
    if (postRC != 200) {
        println("Print content.")
        def output = post.getErrorStream()
        println("Responce error: ${output}")
        throw new Exception("Wrong response.")
    } else {
        def jsonSlurper = new JsonSlurperClassic()
        def output = jsonSlurper.parseText("${post.getInputStream()}")
        println("Print content.")
        println("Responce type: ${output.getClass()}")
        println("Responce: ${output}")
        return output
    }
}

/////////////////////////////////////////////////

// GitHub PR comment management

// `data_pull_request__links_comments_href` variable that available
// in web-hook data.
// Example:
//      data_pull_request__links_comments_href=https://api.github.com/repos/ParticleDev/commodity_research/issues/2223/comments

def postPrComment(String message) {
    // Post comment to the PR that triggered job
    def url = "${data_pull_request__links_comments_href}"
    def payload = [body: message,]
    postRequestGhApi(url, payload)
}

/////////////////////////////////////////////////

// GitHub labels management

// `data_pull_request_issue_url` variable that available
// in web-hook data.
// Example:
//      data_pull_request_issue_url=https://api.github.com/repos/ParticleDev/commodity_research/issues/2223

def labelsGetFromIssue() {
    // Get labels(List of strings) from the PR that triggered job.

    def url = "${data_pull_request_issue_url}/labels"
    def data = getRequestGhApi(url)
    def output = []
    for (i = 0; i < data.size(); i++) {
        output.add(data[i].name)
    }
    return output
}

def labelDeleteFromIssue(String name) {
    // Delete label from the PR that triggered job.
    def url = "${data_pull_request_issue_url}/labels/${name}"
    println("Delete label URL: ${url}")
    deleteRequestGhApi(url)
    return url
}

def labelAddToIssue(String name) {
    // Add label to the PR that triggered job.
    def url = "${data_pull_request_issue_url}/labels"
    println("Add label URL: ${url}")
    def payload = [labels: [name]]
    postRequestGhApi(url, payload)
}

/////////////////////////////////////////////////

// Github statuses management

// `data_pull_request_statuses_url` variable that available
// in web-hook data.
// Example:
//      data_pull_request_statuses_url=https://api.github.com/repos/ParticleDev/commodity_research/statuses/45e1be89cd0dddcd7ad9c3076c8ac3b87b4304e4

def postGitStatus(String state, String description, String testName) {
    // Add status to the PR that triggered job.
    def url = "${data_pull_request_statuses_url}"
    def console_url = "${BUILD_URL}console"
    def payload = [state      : state,
                   description: description,
                   target_url : "${console_url}",
                   context    : "${JOB_NAME} -> ${testName}"]
    postRequestGhApi(url, payload)
}

def statusSetPending(String testName) {
    // Add status `pending` to the PR that triggered job.
    postGitStatus("pending",
            "Job started.",
            testName)
}

def statusSetSuccess(String testName) {
    // Add status `success` to the PR that triggered job.
    postGitStatus("success",
            "Build Successful.",
            testName)
}

def statusSetFail(String testName) {
    // Add status `failure` the PR that triggered job.
    postGitStatus("failure",
            "Test failed.",
            testName)
}

def statusSetError(String testName) {
    // Add status `error` to the PR that triggered job.
    postGitStatus("error",
            "Build error.",
            testName)
}

/////////////////////////////////////////////////

// Environment
// `data` variable that contains all web-hook data.
// Example:
//      data={"action":"opened","number":2223,"pull_request":{"url":"https://api ... "type":"User","site_admin":false}}

def getHookData() {
    // Parse web-hook data from the enviroment variable
    def jsonSlurper = new JsonSlurperClassic()
    def jData = jsonSlurper.parseText(data)
    return jData
}

def getLabels() {
    // Get list of labels name from web-hook data.
    def jData = getHookData()
    def labelsList = jData.pull_request.labels
    def labels = []
    for (int i = 0; i < labelsList.size(); i++) {
        labels.add(labelsList.get(i).name)
    }
    return labels
}

def getLabel() {
    // Get label that were attached to the pr.
    // Available only when action="labeled"
    def jData = getHookData()
    return jData.label.name
}

def getAction() {
    // Get web-hook action.
    // Actions for PR:
    // opened, closed, reopened, edited, assigned, unassigned, review requested,
    //review request removed, labeled, unlabeled, synchronized, ready for review,
    //locked, or unlocked
    jData = getHookData()
    return jData.action
}

/////////////////////////////////////////////////

// Triggers

def getLinterTrigger() {
    // Get trigger for linter tests.
    def action = getAction()
    def trigger = false
    if (action == 'created') {
        trigger = true
    }
    if (action == 'opened') {
        trigger = true
    }
    if (action == 'labeled') {
        def label = getLabel()
        if (label == 'PR_run_linter_tests') {
            trigger = true
        }
    }
    return trigger
}

def getFastTestsTrigger() {
    // Get trigges for fast tests
    def action = getAction()
    def trigger = false
    if (action == 'created') {
        trigger = true
    }
    if (action == 'opened') {
        trigger = true
    }
    if (action == 'labeled') {
        def label = getLabel()
        if (label == 'PR_run_fast_tests') {
            trigger = true
        }
    }
    return trigger
}

def getSlowTestsTrigger() {
    // Get trigger for slow tests
    def trigger = false
    def action = getAction()
    if (action == 'labeled') {
        def label = getLabel()
        if (label == 'PR_run_slow_tests') {
            trigger = true
        }
    }
    return trigger
}

////////////////////////////////////////////////////////

// Utils

def getFileMessage(String wd) {
    // Read message from tmp file.
    def path = wd + "/tmp_message.txt"
    File file = new File(path)
    String message = file.text

    return message
}

def getFileExitStatus(String wd) {
    // Read exit status from tmp file.
    def path = wd + "/tmp_exit_status.txt"
    File file = new File(path)
    String status_code = file.text
    return status_code
}

def printInfo() {
    // Print info message.
    def output = """
        // INFO MESSAGE //
        Build ID    : ${BUILD_ID}
        Workspace   : ${WORKSPACE}
        Repo URL    : ${GIT_URL}
        Branch      : ${GIT_BRANCH}
        PR URL      : ${data_pull_request__links_html_href}
        Commit      : ${GIT_COMMIT}
        Action      : ${data_action}
        Labels      : ${getLabels()}
        Triggers:
            linter_tests    : ${getLinterTrigger()}
            fast_testf      : ${getFastTestsTrigger()}
            slow_tests      : ${getSlowTestsTrigger()}
        // END INFO MESSAGE //
"""
    println(output)
}

/////////////////////////////////////////////////

// Settings

def getLinterTestName() {
    return "linter_tests"
}

def getLinterWorkspacePath() {
    return "${WORKSPACE}/${BUILD_NUMBER}/${getLinterTestName()}"
}

def getFastTestName() {
    return "fast_tests"
}

def getFastTestWorkspacePath() {
    return "${WORKSPACE}/${BUILD_NUMBER}/${getFastTestName()}"
}

def getSlowTestName() {
    return "slow_tests"
}

def getSlowTestWorkspacePath() {
    return "${WORKSPACE}/${BUILD_NUMBER}/${getSlowTestName()}"
}

pipeline {
    agent any
    stages {
        stage('Print env') {
            steps {
                printInfo()
                sh '''env | grep -v 'data={"action"' | sort'''
            }
        }
        stage("Run linter tests") {
            when {
                expression { getLinterTrigger() }
            }
            steps {
                // Print settings
                script {
                    echo("""
                            LinterJobSettings:
                                jobName         : ${getLinterTestName()}
                                jobWorkspace    : ${getLinterWorkspacePath()}
                                labels          : ${labelsGetFromIssue()})
                                """)
                }
                // Remove labels
                script {
                    statusSetPending(getLinterTestName())
                    labelDeleteFromIssue('PR_run_linter_tests')
                    labelDeleteFromIssue('linter_dirty')
                    labelDeleteFromIssue('linter_clean')
                }
                // Pull project
                script {
                    // Remove `PR_run_linter_tests` label and pull project
                    try {
                        dir(getLinterWorkspacePath()) {
                            sh '''git clone ${data_pull_request_base_repo_ssh_url} --recurse-submodules .'''
                            sh '''git checkout ${data_pull_request_head_sha} --recurse-submodules'''
                            sh '''git status'''
                        }

                    }
                    catch (exc) {
                        script {
                            statusSetError(getLinterTestName())
                            throw exc
                        }
                    }
                }
                // Run linter tests
                script {
                    try {
                        dir(getLinterWorkspacePath()) {
                            sh('''
                                    bash -c "source dev_scripts/jenkins/amp.run_linter_on_branch.sh"
                                    ''')
//                            sh('''printf "0" > ./tmp_exit_status.txt''')
//                            sh('''printf "0" > ./tmp_message.txt''')
                            // TODO: Remove after tests
                            def msg = getFileMessage(getLinterWorkspacePath())
                            def exitStatus = getFileExitStatus(getLinterWorkspacePath())
                            echo("message=${msg}")
                            echo("exitStatus=`${exitStatus}`")
                            postPrComment(msg)

                            echo('Linter tests done.')
                            script {
                                echo("Exit status type: ${exitStatus.getClass()}")
                                if (exitStatus == "0") {
                                    labelAddToIssue('linter_clean')

                                } else {
                                    labelAddToIssue('linter_dirty')
                                }
                            }
                        }
                        script {
                            statusSetSuccess(getLinterTestName())
                        }
                    }
                    catch (exc) {
                        script {
                            statusSetError(getLinterTestName())
                            labelAddToIssue('linter_dirty')
                            throw exc
                        }
                    }
                }
                dir(getLinterWorkspacePath()) {

                    deleteDir()
                }

            }

        }
        stage("Run fast tests") {
            when {
                expression { getFastTestsTrigger() }
            }
            steps {
                // Print settings
                script {
                    echo("""
                            FastTestsJobSettings:
                                jobName         : ${getFastTestName()}
                                jobWorkspace    : ${getFastTestWorkspacePath()}
                                """)
                }
                // Remove labels
                script {
                    statusSetPending(getFastTestName())
                    labelDeleteFromIssue('PR_run_fast_tests')
                    labelDeleteFromIssue('PR_to_review')
                    labelDeleteFromIssue('fast_tests_fail')
                    labelDeleteFromIssue('fast_tests_pass')
                }
                // Pull project
                script {
                    // Remove `PR_run_linter_tests` label and pull project
                    try {
                        dir(getFastTestWorkspacePath()) {
                            sh '''git clone ${data_pull_request_base_repo_ssh_url} --recurse-submodules .'''
                            sh '''git checkout ${data_pull_request_head_sha} --recurse-submodules'''
                            sh '''git status'''
                        }

                    }
                    catch (exc) {
                        script {
                            statusSetError(getFastTestName())
                            throw exc
                        }
                    }
                }
                // Run fast tests
                script {
                    try {
                        dir(getFastTestWorkspacePath()) {
                            sh('''
                                    bash -c "source dev_scripts/jenkins/amp.run_fast_tests.sh"
                                    ''')
//                            sh('''printf "0" > ./tmp_exit_status.txt''')
                            // TODO: delete after tests
                            def exitStatus = getFileExitStatus(getFastTestWorkspacePath())
                            echo("exit_status=`${exitStatus}`")

                            echo('Fast tests done.')
                            script {
                                echo("Exit status type: ${exitStatus.getClass()}")
                                if (exitStatus == "0") {
                                    echo("Entering success branch.")
                                    labelAddToIssue('fast_tests_pass')
                                    statusSetSuccess(getFastTestName())
                                    def remoteLabels = labelsGetFromIssue()
                                    if (remoteLabels.contains("slow_tests_fail")) {
                                        echo("There are fail labels.")
                                        echo("Labels: ${remoteLabels}")
                                    } else {
                                        labelAddToIssue('PR_to_review')
                                    }
                                } else {
                                    echo("Entering failure branch.")
                                    labelAddToIssue('fast_tests_fail')
                                    statusSetFail(getFastTestName())
                                    currentBuild.result = 'FAILURE'
                                }
                            }
                        }
                    }
                    catch (exc) {
                        script {
                            statusSetError(getFastTestName())
                            labelAddToIssue('fast_tests_fail')
                            throw exc
                        }
                    }

                }
                dir(getFastTestWorkspacePath()) {

                    deleteDir()
                }
            }

        }
        stage("Run slow tests") {
            when {
                expression { getSlowTestsTrigger() }
            }
            steps {
                // Print settings
                script {
                    echo("""
                            SlowTestsJobSettings:
                                jobName         : ${getSlowTestName()}
                                jobWorkspace    : ${getSlowTestWorkspacePath()}
                                """)
                }
                // Remove labels
                script {
                    statusSetPending(getSlowTestName())
                    labelDeleteFromIssue('PR_run_slow_tests')
                    labelDeleteFromIssue('PR_to_review')
                    labelDeleteFromIssue('slow_tests_fail')
                    labelDeleteFromIssue('slow_tests_pass')
                }
                // Pull project
                script {
                    // Remove `PR_run_linter_tests` label and pull project
                    try {
                        dir(getSlowTestWorkspacePath()) {
                            sh '''git clone ${data_pull_request_base_repo_ssh_url} --recurse-submodules .'''
                            sh '''git checkout ${data_pull_request_head_sha} --recurse-submodules'''
                            sh '''git status'''
                        }

                    }
                    catch (exc) {
                        script {
                            statusSetError(getSlowTestName())
                            throw exc
                        }
                    }
                }
                // Run slow tests
                script {
                    try {
                        dir(getSlowTestWorkspacePath()) {
                            sh('''
                                    bash -c "source dev_scripts/jenkins/amp.run_slow_tests.sh"
                                    ''')
//                            sh('''printf "0" > ./tmp_exit_status.txt''')
                            // TODO: delete after tests
                            def exitStatus = getFileExitStatus(getSlowTestWorkspacePath())
                            echo("exit_status=`${exitStatus}`")

                            echo('Slow tests done.')
                            script {
                                echo("Exit status type: ${exitStatus.getClass()}")
                                if (exitStatus == "0") {
                                    echo("Entering success branch.")
                                    labelAddToIssue('slow_tests_pass')
                                    statusSetSuccess(getSlowTestName())
                                    def remoteLabels = labelsGetFromIssue()
                                    if (remoteLabels.contains("fast_tests_fail")) {
                                        echo("There are fail labels.")
                                        echo("Labels: ${remoteLabels}")
                                    } else {
                                        labelAddToIssue('PR_to_review')
                                    }
                                } else {
                                    echo("Entering failure branch.")
                                    labelAddToIssue('slow_tests_fail')
                                    statusSetFail(getSlowTestName())
                                    currentBuild.result = 'FAILURE'
                                }
                            }
                            dir(getSlowTestWorkspacePath()) {
                                deleteDir()
                            }
                        }
                    }
                    catch (exc) {
                        script {
                            statusSetError(getSlowTestName())
                            labelAddToIssue('slow_tests_fail')
                            throw exc
                        }
                    }
                }
            }
        }

    }
}

