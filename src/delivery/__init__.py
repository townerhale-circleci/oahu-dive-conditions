"""Message delivery via SMS and email."""

from src.delivery.twilio_sender import (
    SMSResult,
    TwilioError,
    TwilioSender,
    send_digest_sms,
    send_sms,
)
from src.delivery.sendgrid_sender import (
    EmailResult,
    SendGridError,
    SendGridSender,
    send_digest_email,
    send_email,
)

__all__ = [
    # Twilio
    "SMSResult",
    "TwilioError",
    "TwilioSender",
    "send_digest_sms",
    "send_sms",
    # SendGrid
    "EmailResult",
    "SendGridError",
    "SendGridSender",
    "send_digest_email",
    "send_email",
]
