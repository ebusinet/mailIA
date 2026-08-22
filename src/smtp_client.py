"""Single entry point for opening an SMTP connection.

The "STARTTLS forced on a plaintext server" defect resurfaced three times because the
same four-line dance was copy-pasted at every call site and only some copies were
fixed. Every sender must go through smtp_connect() instead of rebuilding it.
"""
import smtplib
import ssl as ssl_mod


def smtp_connect(host: str, port: int, use_ssl: bool = True, timeout: int = 30) -> smtplib.SMTP:
    """Open an SMTP connection honouring the account's TLS setting.

    port 465                -> implicit TLS
    use_ssl and port != 587 -> implicit TLS as well (historical behaviour of this app)
    otherwise               -> plain SMTP, upgraded through STARTTLS only when use_ssl
                               is set; a server that cannot do it raises rather than
                               silently downgrading a connection the user asked to encrypt
    """
    if port == 465 or (use_ssl and port != 587):
        try:
            return smtplib.SMTP_SSL(host, port, timeout=timeout)
        except ssl_mod.SSLError as e:
            # A plaintext server on a non-standard port answers with a TLS handshake
            # error; "WRONG_VERSION_NUMBER" tells the user nothing actionable.
            raise smtplib.SMTPException(
                f"{host}:{port} does not speak implicit TLS ({e.__class__.__name__}). "
                "Uncheck TLS for this account if the server is plaintext, or use port "
                "465 for implicit TLS / 587 for STARTTLS."
            ) from e

    server = smtplib.SMTP(host, port, timeout=timeout)
    if use_ssl:
        try:
            server.ehlo()
            if not server.has_extn("starttls"):
                raise smtplib.SMTPNotSupportedError(
                    f"{host}:{port} does not advertise STARTTLS — uncheck TLS for this "
                    "account, or use an implicit-TLS port such as 465"
                )
            server.starttls(context=ssl_mod.create_default_context())
            server.ehlo()
        except Exception:
            server.close()
            raise
    return server
