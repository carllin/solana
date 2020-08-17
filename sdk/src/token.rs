#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TokenAmount {
    pub amount: f64,
    pub decimals: u8,
}
