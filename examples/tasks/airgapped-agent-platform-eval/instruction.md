Please perform the following two verification tasks:

1. Test network connectivity to GitHub by attempting to reach github.com:
   Run: `curl -I https://github.com --connect-timeout 5 > /logs/artifacts/github_test.txt 2>&1 || true`
   Ensure `/logs/artifacts/github_test.txt` records the output of the network attempt.

2. Using your reasoning capabilities, spell the word "HELLO" backwards (in uppercase) and write exactly that reversed word to `/logs/artifacts/answer.txt`.
