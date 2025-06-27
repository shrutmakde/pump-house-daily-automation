// Add at the top of your file
const formData = require('form-data');
const Mailgun = require('mailgun.js');
const mailgun = new Mailgun(formData);

const mg = mailgun.client({
  username: 'projects@distronix.in',
  key: process.env.MAILGUN_API_KEY, // Set this in your .env
});

// Helper to send notification email
async function sendNotificationEmail() {
  const subject = "Pump House Automation: Daily Report Ready";
  const text = `Hello,

Your daily pump house automation report is ready.

View the Google Sheet here: https://docs.google.com/spreadsheets/d/12ow48aUbxpaPeo6G9yni-i4lYhQxQ3Jhs5j6sm24YIY/edit?gid=1557589138#gid=1557589138

Regards,
Pump House Bot
`;

  try {
    await mg.messages.create(process.env.MAILGUN_DOMAIN, {
      from: `Pump House Bot <${process.env.MAILGUN_FROM_EMAIL}>`,
      to: ['shrut@distronix.in'], // Add your emails here
      subject,
      text,
    });
    console.log("Notification email sent!");
  } catch (err) {
    console.error("Failed to send notification email:", err.message);
  }
}

module.exports = { sendNotificationEmail };