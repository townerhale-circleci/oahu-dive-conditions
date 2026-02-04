"""Email delivery via SendGrid.

Sends dive condition digests as HTML or plain text emails.
Requires environment variable:
- SENDGRID_API_KEY
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


class SendGridError(Exception):
    """Error sending email via SendGrid."""
    pass


@dataclass
class EmailResult:
    """Result of sending an email."""
    success: bool
    to_email: str
    status_code: Optional[int] = None
    message_id: Optional[str] = None
    error: Optional[str] = None


class SendGridSender:
    """Sends emails via SendGrid."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        from_email: Optional[str] = None,
        from_name: str = "Oahu Dive Conditions",
    ):
        """Initialize SendGrid sender.

        Args:
            api_key: SendGrid API key. Defaults to SENDGRID_API_KEY env var.
            from_email: Sender email address. Defaults to SENDGRID_FROM_EMAIL env var.
            from_name: Sender display name.
        """
        self.api_key = api_key or os.environ.get("SENDGRID_API_KEY")
        self.from_email = from_email or os.environ.get("SENDGRID_FROM_EMAIL", "dive-conditions@example.com")
        self.from_name = from_name

        self._client = None

    @property
    def is_configured(self) -> bool:
        """Check if SendGrid credentials are configured."""
        return bool(self.api_key)

    def _get_client(self):
        """Get or create SendGrid client."""
        if self._client is None:
            if not self.is_configured:
                raise SendGridError(
                    "SendGrid not configured. Set SENDGRID_API_KEY environment variable."
                )
            try:
                from sendgrid import SendGridAPIClient
                self._client = SendGridAPIClient(self.api_key)
            except ImportError:
                raise SendGridError("sendgrid package not installed. Run: pip install sendgrid")
        return self._client

    def send(
        self,
        to_email: str,
        subject: str,
        html_content: Optional[str] = None,
        text_content: Optional[str] = None,
    ) -> EmailResult:
        """Send an email.

        Args:
            to_email: Recipient email address
            subject: Email subject
            html_content: HTML body content
            text_content: Plain text body content

        Returns:
            EmailResult with success status and details
        """
        if not html_content and not text_content:
            raise SendGridError("Either html_content or text_content must be provided")

        try:
            from sendgrid.helpers.mail import Mail, Email, To, Content

            message = Mail(
                from_email=Email(self.from_email, self.from_name),
                to_emails=To(to_email),
                subject=subject,
            )

            if html_content:
                message.add_content(Content("text/html", html_content))
            if text_content:
                message.add_content(Content("text/plain", text_content))

            client = self._get_client()
            response = client.send(message)

            success = 200 <= response.status_code < 300
            message_id = response.headers.get("X-Message-Id")

            if success:
                logger.info(f"Email sent to {to_email}: {message_id}")
            else:
                logger.warning(f"Email to {to_email} returned status {response.status_code}")

            return EmailResult(
                success=success,
                to_email=to_email,
                status_code=response.status_code,
                message_id=message_id,
            )

        except SendGridError:
            raise
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to send email to {to_email}: {error_msg}")
            return EmailResult(
                success=False,
                to_email=to_email,
                error=error_msg,
            )

    def send_bulk(
        self,
        recipients: list[str],
        subject: str,
        html_content: Optional[str] = None,
        text_content: Optional[str] = None,
    ) -> list[EmailResult]:
        """Send email to multiple recipients.

        Args:
            recipients: List of email addresses
            subject: Email subject
            html_content: HTML body content
            text_content: Plain text body content

        Returns:
            List of EmailResult for each recipient
        """
        results = []
        for email in recipients:
            result = self.send(
                to_email=email,
                subject=subject,
                html_content=html_content,
                text_content=text_content,
            )
            results.append(result)
        return results

    def send_digest(
        self,
        recipients: list[str],
        subject: str,
        html_content: str,
        text_content: str,
    ) -> dict:
        """Send a formatted digest to multiple recipients.

        Args:
            recipients: List of email addresses
            subject: Email subject
            html_content: HTML-formatted digest (from DigestFormatter.format_email_html())
            text_content: Plain text digest (from DigestFormatter.format_email_text())

        Returns:
            Dict with 'sent', 'failed', and 'results' keys
        """
        results = self.send_bulk(
            recipients=recipients,
            subject=subject,
            html_content=html_content,
            text_content=text_content,
        )

        sent = sum(1 for r in results if r.success)
        failed = len(results) - sent

        return {
            "sent": sent,
            "failed": failed,
            "total": len(results),
            "results": results,
        }


def send_email(
    to_email: str,
    subject: str,
    html_content: Optional[str] = None,
    text_content: Optional[str] = None,
) -> EmailResult:
    """Convenience function to send a single email.

    Args:
        to_email: Recipient email address
        subject: Email subject
        html_content: HTML body content
        text_content: Plain text body content

    Returns:
        EmailResult
    """
    sender = SendGridSender()
    return sender.send(to_email, subject, html_content, text_content)


def send_digest_email(
    recipients: list[str],
    html_content: str,
    text_content: str,
    subject: str = "Oahu Dive Conditions Daily Report",
) -> dict:
    """Convenience function to send digest to multiple recipients.

    Args:
        recipients: List of email addresses
        html_content: HTML-formatted digest
        text_content: Plain text digest
        subject: Email subject

    Returns:
        Dict with send results
    """
    sender = SendGridSender()
    return sender.send_digest(recipients, subject, html_content, text_content)
