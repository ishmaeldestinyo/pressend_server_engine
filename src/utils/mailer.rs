use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor, message::header::ContentType,
    transport::smtp::authentication::Credentials,
};
use std::sync::OnceLock;
use tera::{Context, Tera};

use crate::modules::legacy_plan::schemas::LegacyBeneficiarySummary;

static TEMPLATES: OnceLock<Tera> = OnceLock::new();

pub fn get_templates() -> &'static Tera {
    TEMPLATES.get_or_init(|| {
        let pattern = format!(
            "{}/templates/**/*.html",
            std::env::current_dir()
                .expect("Failed to get current dir")
                .display()
        );
        let tera = Tera::new(&pattern).expect("❌ Failed to load email templates");
        println!(
            "Loaded templates: {:?}",
            tera.get_template_names().collect::<Vec<_>>()
        );
        tera
    })
}

pub struct Mailer {
    transport: AsyncSmtpTransport<Tokio1Executor>,
    from: String,
    app_name: String,
    support_email: String,
}

impl Mailer {
    pub fn new(
        mail_server: &str,
        mail_port: u16,
        mail_user: &str,
        mail_password: &str,
        app_name: &str,
        support_email: &str,
    ) -> Self {
        let creds = Credentials::new(mail_user.to_string(), mail_password.to_string());

        let transport: AsyncSmtpTransport<Tokio1Executor> =
            AsyncSmtpTransport::<Tokio1Executor>::relay(&mail_server)
                .unwrap()
                .port(mail_port)
                .credentials(creds)
                .build();

        Self {
            transport,
            from: mail_user.to_string(),
            app_name: app_name.to_string(),
            support_email: support_email.to_string(),
        }
    }

    pub async fn send(
        &self,
        to: &str,
        subject: &str,
        template_name: &str,
        context: Context,
    ) -> Result<(), String> {
        let tera = get_templates();

        let html = tera
            .render(template_name, &context)
            .map_err(|e| format!("Template error: {}", e))?;

        let email = Message::builder()
            .from(
                format!("{} <{}>", self.app_name, self.from)
                    .parse()
                    .unwrap(),
            )
            .to(to.parse().map_err(|e| format!("Invalid email: {}", e))?)
            .subject(subject)
            .header(ContentType::TEXT_HTML)
            .body(html)
            .map_err(|e| format!("Email build error: {}", e))?;

        self.transport
            .send(email)
            .await
            .map_err(|e| format!("Send error: {}", e))?;

        Ok(())
    }

    pub async fn send_otp(&self, to: &str, firstname: &str, otp: &str) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("otp", otp);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert("expires_in", "10 minutes");

        self.send(to, "Verify your email", "verification.html", ctx)
            .await
    }

    pub async fn send_welcome(&self, to: &str, firstname: &str) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);

        self.send(
            to,
            &format!("Welcome to {}!", self.app_name),
            "welcome_email.html",
            ctx,
        )
        .await
    }

    pub async fn send_password_changed(&self, to: &str, firstname: &str) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "changed_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("Your {} password was changed", self.app_name),
            "password_changed.html",
            ctx,
        )
        .await
    }

    pub async fn send_account_deleted(
        &self,
        to: &str,
        firstname: &str,
        deletion_reason: Option<String>,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("email", to);
        ctx.insert("support_email", &self.support_email);
        ctx.insert("deletion_reason", &deletion_reason.unwrap_or_default());
        ctx.insert(
            "deleted_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("Your {} account has been closed", self.app_name),
            "account_deleted.html",
            ctx,
        )
        .await
    }

    pub async fn send_suspicious_login(
        &self,
        to: &str,
        firstname: &str,
        ip: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("email", to);
        ctx.insert("ip", ip);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "attempted_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("Suspicious login attempt on your {} account", self.app_name),
            "suspicious_login.html",
            ctx,
        )
        .await
    }

    pub async fn send_new_device_otp(
        &self,
        to: &str,
        firstname: &str,
        otp: &str,
        ip: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("otp", otp);
        ctx.insert("ip", ip);
        ctx.insert("support_email", &self.support_email);
        ctx.insert("expires_in", "10 minutes");
        ctx.insert(
            "attempted_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("New device login attempt - {}", self.app_name),
            "new_device_otp.html",
            ctx,
        )
        .await
    }

    pub async fn send_account_loggedin_email(
        &self,
        to: &str,
        firstname: &str,
        ip: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("email", to);
        ctx.insert("ip", ip);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "attempted_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("Login Notification on your {} account", self.app_name),
            "login_notification.html",
            ctx,
        )
        .await
    }

    pub async fn send_email_changed(
        &self,
        to: &str,
        firstname: &str,
        old_email: &str,
    ) -> Result<(), String> {
        fn mask_email(email: &str) -> String {
            if let Some(at_pos) = email.find('@') {
                let local = &email[..at_pos];
                let domain = &email[at_pos..];
                let len = local.len();
                if len <= 2 {
                    format!("{}*{}", &local[..1], domain)
                } else {
                    format!(
                        "{}{}{}{}",
                        &local[..1],
                        "*".repeat(len - 2),
                        &local[len - 1..],
                        domain
                    )
                }
            } else {
                "*".repeat(email.len())
            }
        }

        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("old_email", &mask_email(old_email));
        ctx.insert("new_email", &mask_email(to));
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "changed_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("Your {} email address has been updated", self.app_name),
            "email_changed.html",
            ctx,
        )
        .await
    }

    pub async fn send_tier_upgrade_approved(
        &self,
        to: &str,
        firstname: &str,
        tier: u8,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("tier", &tier);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "approved_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!(
                "Your {} Tier {} upgrade has been approved",
                self.app_name, tier
            ),
            "tier_upgrade_approved.html",
            ctx,
        )
        .await
    }

    pub async fn send_tier_upgrade_rejected(
        &self,
        to: &str,
        firstname: &str,
        tier: u8,
        reason: Option<&str>,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("tier", &tier);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "reason",
            &reason.unwrap_or("Your submitted information could not be verified."),
        );
        ctx.insert(
            "rejected_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!(
                "Your {} Tier {} upgrade was unsuccessful",
                self.app_name, tier
            ),
            "tier_upgrade_rejected.html",
            ctx,
        )
        .await
    }

    pub async fn send_legacy_beneficiary_added(
        &self,
        to: &str,
        beneficiary_name: &str,
        owner_name: &str,
        share_percentage: f64,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("beneficiary_name", beneficiary_name);
        ctx.insert("owner_name", owner_name);
        ctx.insert("share_percentage", &share_percentage);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "added_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("You have been added as a beneficiary on {}", self.app_name),
            "posthumous_beneficiary_added.html",
            ctx,
        )
        .await
    }

    pub async fn send_legacy_beneficiary_deleted(
        &self,
        to: &str,
        beneficiary_name: &str,
        owner_name: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("beneficiary_name", beneficiary_name);
        ctx.insert("owner_name", owner_name);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "deleted_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!(
                "Your beneficiary status on {} has been cancelled",
                self.app_name
            ),
            "posthumous_beneficiary_deleted.html",
            ctx,
        )
        .await
    }

    pub async fn send_legacy_grace_period(
        &self,
        to: &str,
        firstname: &str,
        inactivity_days: i32,
        grace_period_days: i32,
        beneficiary_count: usize,
        execution_date: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert("firstname", firstname);
        ctx.insert("inactivity_days", &inactivity_days);
        ctx.insert("grace_period_days", &grace_period_days);
        ctx.insert("beneficiary_count", &beneficiary_count);
        ctx.insert("execution_date", execution_date);

        self.send(
            to,
            &format!(" Your {} Legacy Plan Has Been Triggered", self.app_name),
            "legacy_grace_period.html",
            ctx,
        )
        .await
    }

    pub async fn send_legacy_executed_owner(
        &self,
        to: &str,
        firstname: &str,
        total_distributed: &str,
        beneficiary_count: usize,
        beneficiaries: &[LegacyBeneficiarySummary],
        execution_date: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert("firstname", firstname);
        ctx.insert("total_distributed", total_distributed);
        ctx.insert("beneficiary_count", &beneficiary_count);
        ctx.insert("beneficiaries", beneficiaries);
        ctx.insert("execution_date", execution_date);

        self.send(
            to,
            &format!(" Your {} Legacy Plan Has Been Executed", self.app_name),
            "legacy_executed_owner.html",
            ctx,
        )
        .await
    }

    pub async fn send_legacy_executed_beneficiary(
        &self,
        to: &str,
        beneficiary_name: &str,
        owner_name: &str,
        net_amount: &str,
        share_percentage: f64,
        transfer_type: &str,
        reference: &str,
        execution_date: &str,
    ) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert("beneficiary_name", beneficiary_name);
        ctx.insert("owner_name", owner_name);
        ctx.insert("net_amount", net_amount);
        ctx.insert("share_percentage", &share_percentage);
        ctx.insert("transfer_type", transfer_type);
        ctx.insert("reference", reference);
        ctx.insert("execution_date", execution_date);

        self.send(
            to,
            " You Have Received a Legacy Transfer",
            "legacy_executed_beneficiary.html",
            ctx,
        )
        .await
    }

    pub async fn send_payment_pin_set(&self, to: &str, firstname: &str) -> Result<(), String> {
        let mut ctx = Context::new();
        ctx.insert("firstname", firstname);
        ctx.insert("app_name", &self.app_name);
        ctx.insert("support_email", &self.support_email);
        ctx.insert(
            "changed_at",
            &chrono::Utc::now()
                .format("%B %d, %Y at %H:%M UTC")
                .to_string(),
        );

        self.send(
            to,
            &format!("Your {} payment PIN has been set", self.app_name),
            "payment_pin_set.html",
            ctx,
        )
        .await
    }
}
