pub struct Subscription {
    pub id: i32,
    pub name: String,
    pub topic: String,
    pub status: String,
    pub created_at: NaiveDateTime,
    pub updated_at: NaiveDateTime,
}

impl Subscription {
    pub fn new(
        id: i32,
        name: String,
        topic: String,
        status: String,
        created_at: NaiveDateTime,
        updated_at: NaiveDateTime,
    ) -> Self {
        Subscription {
            id,
            name,
            topic,
            status,
            created_at,
            updated_at,
        }
    }
}
