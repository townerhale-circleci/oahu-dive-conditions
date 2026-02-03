"""SMS delivery via Twilio.

Sends dive condition digests as SMS messages.
Requires environment variables:
- TWILIO_ACCOUNT_SID
- TWILIO_AUTH_TOKEN
- TWILIO_FROM_NUMBER
"""

import logging
import os
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)


class TwilioError(Exception):
    """Error sending SMS via Twilio."""
    pass


@dataclass
class SMSResult:
    """Result of sending an SMS."""
    success: bool
    to_number: str
    message_sid: Optional[str] = None
    error: Optional[str] = None
    segments: int = 1


class TwilioSender:
    """Sends SMS messages via Twilio."""

    SMS_SEGMENT_LENGTH = 160
    MAX_SEGMENTS = 10  # Limit to 10 segments (1600 chars)

    def __init__(
        self,
        account_sid: Optional[str] = None,
        auth_token: Optional[str] = None,
        from_number: Optional[str] = None,
    ):
        """Initialize Twilio sender.

        Args:
            account_sid: Twilio account SID. Defaults to TWILIO_ACCOUNT_SID env var.
            auth_token: Twilio auth token. Defaults to TWILIO_AUTH_TOKEN env var.
            from_number: Twilio phone number. Defaults to TWILIO_FROM_NUMBER env var.
        """
        self.account_sid = account_sid or os.environ.get("TWILIO_ACCOUNT_SID")
        self.auth_token = auth_token or os.environ.get("TWILIO_AUTH_TOKEN")
        self.from_number = from_number or os.environ.get("TWILIO_FROM_NUMBER")

        self._client = None

    @property
    def is_configured(self) -> bool:
        """Check if Twilio credentials are configured."""
        return all([self.account_sid, self.auth_token, self.from_number])

    def _get_client(self):
        """Get or create Twilio client."""
        if self._client is None:
            if not self.is_configured:
                raise TwilioError(
                    "Twilio not configured. Set TWILIO_ACCOUNT_SID, "
                    "TWILIO_AUTH_TOKEN, and TWILIO_FROM_NUMBER environment variables."
                )
            try:
                from twilio.rest import Client
                self._client = Client(self.account_sid, self.auth_token)
            except ImportError:
                raise TwilioError("twilio package not installed. Run: pip install twilio")
        return self._client

    def send(self, to_number: str, message: str) -> SMSResult:
        """Send an SMS message.

        Args:
            to_number: Recipient phone number (E.164 format, e.g., +18081234567)
            message: Message text

        Returns:
            SMSResult with success status and details
        """
        # Validate phone number format
        if not to_number.startswith("+"):
            to_number = f"+1{to_number}"  # Assume US number

        # Calculate segments
        segments = (len(message) + self.SMS_SEGMENT_LENGTH - 1) // self.SMS_SEGMENT_LENGTH

        # Truncate if too long
        max_length = self.MAX_SEGMENTS * self.SMS_SEGMENT_LENGTH
        if len(message) > max_length:
            message = message[:max_length - 3] + "..."
            logger.warning(f"Message truncated to {max_length} characters")

        try:
            client = self._get_client()

            twilio_message = client.messages.create(
                body=message,
                from_=self.from_number,
                to=to_number,
            )

            logger.info(f"SMS sent to {to_number}: {twilio_message.sid}")

            return SMSResult(
                success=True,
                to_number=to_number,
                message_sid=twilio_message.sid,
                segments=segments,
            )

        except TwilioError:
            raise
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to send SMS to {to_number}: {error_msg}")
            return SMSResult(
                success=False,
                to_number=to_number,
                error=error_msg,
                segments=segments,
            )

    def send_bulk(self, recipients: list[str], message: str) -> list[SMSResult]:
        """Send SMS to multiple recipients.

        Args:
            recipients: List of phone numbers
            message: Message text

        Returns:
            List of SMSResult for each recipient
        """
        results = []
        for number in recipients:
            result = self.send(number, message)
            results.append(result)
        return results

    def send_digest(
        self,
        recipients: list[str],
        digest_text: str,
    ) -> dict:
        """Send a formatted digest to multiple recipients.

        Args:
            recipients: List of phone numbers
            digest_text: Pre-formatted digest text (from DigestFormatter.format_sms())

        Returns:
            Dict with 'sent', 'failed', and 'results' keys
        """
        results = self.send_bulk(recipients, digest_text)

        sent = sum(1 for r in results if r.success)
        failed = len(results) - sent

        return {
            "sent": sent,
            "failed": failed,
            "total": len(results),
            "results": results,
        }


def send_sms(to_number: str, message: str) -> SMSResult:
    """Convenience function to send a single SMS.

    Args:
        to_number: Recipient phone number
        message: Message text

    Returns:
        SMSResult
    """
    sender = TwilioSender()
    return sender.send(to_number, message)


def send_digest_sms(recipients: list[str], digest_text: str) -> dict:
    """Convenience function to send digest to multiple recipients.

    Args:
        recipients: List of phone numbers
        digest_text: Formatted digest text

    Returns:
        Dict with send results
    """
    sender = TwilioSender()
    return sender.send_digest(recipients, digest_text)
