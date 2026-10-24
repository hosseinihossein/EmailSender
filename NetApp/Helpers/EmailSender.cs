using MimeKit;
using MailKit.Net.Smtp;

namespace NetApp.Helpers
{
    public interface IEmailSender
    {
        Task SendEmailAsync(string receiverUserName, string receiverEmail, string subject, string message);
        void SendEmail(string receiverUserName, string receiverEmail, string subject, string message);
    }

    public class EmailSender : IEmailSender
    {
        private readonly IConfiguration configuration;
        public EmailSender(IConfiguration conf)
        {
            configuration = conf;
        }

        public async Task SendEmailAsync(string receiverUserName, string receiverEmail,
        string subject, string message)
        {
            string senderEmail = configuration["EmailSender:Address"]!;
            string senderGmailPassword = configuration["EmailSender:Password"]!;
            string senderName = "no-reply";

            MimeMessage email = new MimeMessage();
            email.From.Add(new MailboxAddress(senderName, senderEmail));
            email.To.Add(new MailboxAddress(receiverUserName, receiverEmail));
            email.Subject = subject;
            email.Body = new TextPart(MimeKit.Text.TextFormat.Html)
            {
                Text = message,
            };

            using (SmtpClient smtp = new SmtpClient())
            {
                await smtp.ConnectAsync("smtp.gmail.com", 587, MailKit.Security.SecureSocketOptions.Auto);
                await smtp.AuthenticateAsync(senderEmail, senderGmailPassword);
                await smtp.SendAsync(email);
                await smtp.DisconnectAsync(true);
            }

        }

        public void SendEmail(string receiverUserName, string receiverEmail,
        string subject, string message)
        {
            string senderEmail = configuration["EmailSender:Address"]!;
            string senderGmailPassword = configuration["EmailSender:Password"]!;
            string senderName = "no-reply";

            MimeMessage email = new MimeMessage();
            email.From.Add(new MailboxAddress(senderName, senderEmail));
            email.To.Add(new MailboxAddress(receiverUserName, receiverEmail));
            email.Subject = subject;
            email.Body = new TextPart(MimeKit.Text.TextFormat.Html)
            {
                Text = message,
            };

            using (SmtpClient smtp = new SmtpClient())
            {
                smtp.Connect("smtp.gmail.com", 587, MailKit.Security.SecureSocketOptions.Auto);
                smtp.Authenticate(senderEmail, senderGmailPassword);
                smtp.Send(email);
                smtp.Disconnect(true);
            }

        }
    }
}