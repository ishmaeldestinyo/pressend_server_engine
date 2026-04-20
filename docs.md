blinq_server/
├── src/
│   ├── main.rs
│   ├── config.rs          # app config, env vars
│   ├── errors.rs          # global error types
│   ├── db.rs              # database pool setup
│   ├── redis.rs           # redis pool setup
│   ├── kafka.rs           # kafka producer/consumer setup
│   ├── middleware/
│   │   ├── mod.rs
│   │   ├── auth.rs        # JWT extraction + validation
│   │   └── rate_limit.rs  # governor rate limiting
│   ├── modules/
│   │   ├── mod.rs
│   │   ├── auth/
│   │   │   ├── mod.rs
│   │   │   ├── routes.rs      # POST /auth/register, /auth/login etc
│   │   │   ├── handlers.rs    # controller logic
│   │   │   ├── models.rs      # DB structs
│   │   │   └── schemas.rs     # request/response structs (serde)
│   │   ├── account/
│   │   │   ├── mod.rs
│   │   │   ├── routes.rs
│   │   │   ├── handlers.rs
│   │   │   ├── models.rs
│   │   │   └── schemas.rs
│   │   ├── transfer/
│   │   │   ├── mod.rs
│   │   │   ├── routes.rs
│   │   │   ├── handlers.rs
│   │   │   ├── models.rs
│   │   │   └── schemas.rs
│   │   ├── panic/
│   │   │   ├── mod.rs
│   │   │   ├── routes.rs
│   │   │   ├── handlers.rs
│   │   │   ├── models.rs
│   │   │   └── schemas.rs
│   │   ├── vas/               # value added services
│   │   │   ├── mod.rs
│   │   │   ├── routes.rs
│   │   │   ├── handlers.rs
│   │   │   ├── models.rs
│   │   │   └── schemas.rs
│   │   └── webhook/
│   │       ├── mod.rs
│   │       ├── routes.rs
│   │       └── handlers.rs
│   └── utils/
│       ├── mod.rs
│       ├── jwt.rs         # token generation + validation
│       ├── hash.rs        # argon2 password hashing
│       ├── psb.rs         # 9PSB API client + token manager
│       └── paginate.rs    # pagination helpers


security@blinq.com.ng
privacy@blinq.com.ng
dpo@blinq.com.ng
legal@blinq.com.ng
hello@blinq.com.ng
disputes@blinq.com.ng
ir@blinq.com.ng
merchants@blinq.com.ng