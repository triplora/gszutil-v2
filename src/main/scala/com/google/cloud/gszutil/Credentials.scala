package com.google.cloud.gszutil

object Credentials {
  val serviceAccountId: String =
    "service-account@myproject.iam.gserviceaccount.com"

  val privateKeyPem: String =
    """-----BEGIN PRIVATE KEY-----\n<my pem contents>\n-----END PRIVATE KEY-----\n"""
      .replaceAllLiterally("\\n", "\n")
}
