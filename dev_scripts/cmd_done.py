#!/usr/bin/env python

import helpers.send_email as send_email

subject = message = "done"

# TODO(gp): Somehow retrieve the last command (e.g., using history | last -1)

# TODO(gp): Generalize to multiple users using current user: we can use github
# credentials.
send_email.send_mail(subject, message, "saggese@gmail.com")
#send_email.send_mail(subject, message, "4084311286@messaging.sprintpcs.com")
