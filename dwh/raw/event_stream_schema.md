
# event_stream.csv — Schema Reference

**Layer:** Bronze  
**Source:** Raw event log from loyalty/miles rewards app  
**Grain:** One row per user event

---

## Columns

```yaml
fields:
  - name: event_time
    type: TIMESTAMP
    nullable: false
    description: Event timestamp with microsecond precision (UTC)

  - name: user_id
    type: STRING
    nullable: false
    description: Unique user identifier (format: u_XXXX)

  - name: gender
    type: STRING
    nullable: false
    description: User gender
    values: [Male, Female, Non-binary, Prefer not to say]

  - name: event_type
    type: STRING
    nullable: false
    description: Type of event — see Event Types below
    values: [miles_earned, miles_redeemed, reward_search, like, share]

  - name: transaction_category
    type: STRING
    nullable: true
    description: Spending category for miles events; brand name for reward_search; null for like and share

  - name: miles_amount
    type: FLOAT
    nullable: true
    description: Miles earned or redeemed; null for non-miles events

  - name: platform
    type: STRING
    nullable: false
    description: Client platform
    values: [android, ios, web]

  - name: utm_source
    type: STRING
    nullable: false
    description: Acquisition/attribution channel
    values: [organic, facebook, tiktok, google, referral]

  - name: country
    type: STRING
    nullable: false
    description: User country code
    values: [MY, PH, TH, ID, SG]
```

---

## Notes

- `transaction_category` serves a dual purpose: spending category for miles events, and brand/merchant name for `reward_search` events.
- `miles_amount` is always null for non-transactional events (`like`, `share`, `reward_search`).
- Timestamps are stored as strings in the raw CSV and should be cast to `TIMESTAMP` during DWD ingestion.
