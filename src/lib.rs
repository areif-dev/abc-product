use std::{char, collections::HashMap};

use bigdecimal::BigDecimal;
use chrono::NaiveDate;
use gtin::Gtin;

/// The general ledger that a product belongs to
#[derive(Debug, Clone, PartialEq)]
pub enum SaleLedger {
    Feed,
    FarmSupplies,
    CrashAccount,
    UPSShipping,
    DogLicense,
    PetFood,
    PetSupplies,
    Bait,
    FishingTackle,
    FishingLicense,
    HuntingLicense,
    SnacksAndSodas,
    HuntingSupplies,
    PropaneAndGas,
    ShoesAndClothing,
    LawnAndGarden,
    LawnAndGardenPowerEquipment,
    Livestock,
    LandscapeServices,
    FencingMaterials,
    Vaccines,
    Parts,
    AutomotiveHardware,
    Hardware,
    Plumbing,
    Electrical,
    Tools,
    Paint,
    Housewares,
    BuildingProducts,
    WeldingSupplies,
    Fasteners,
    OutdoorLivingAndAutomotive,
    Gifts,
    FloralArrangement,
    GiftCertificates,
    Services,
    OtherInventory,
    CommissionSales,
    AccountsReceivableDiscounts,
    MiscIncome,
    NorthrupKingCommission,
    InsuranceReimbursement,
    Discounts,
    ManufacturersCoupon,
    Adjustments,
    GrossIncome,
}

impl TryFrom<usize> for SaleLedger {
    type Error = AbcParseError;
    fn try_from(value: usize) -> Result<Self, Self::Error> {
        match value {
            501 => Ok(Self::Feed),
            502 => Ok(Self::FarmSupplies),
            505 => Ok(Self::CrashAccount),
            506 => Ok(Self::UPSShipping),
            509 => Ok(Self::DogLicense),
            510 => Ok(Self::PetFood),
            511 => Ok(Self::PetSupplies),
            512 => Ok(Self::Bait),
            513 => Ok(Self::FishingTackle),
            514 => Ok(Self::FishingLicense),
            515 => Ok(Self::HuntingLicense),
            516 => Ok(Self::SnacksAndSodas),
            517 => Ok(Self::HuntingSupplies),
            518 => Ok(Self::PropaneAndGas),
            519 => Ok(Self::ShoesAndClothing),
            520 => Ok(Self::LawnAndGarden),
            521 => Ok(Self::LawnAndGardenPowerEquipment),
            522 => Ok(Self::Livestock),
            523 => Ok(Self::LandscapeServices),
            524 => Ok(Self::FencingMaterials),
            525 => Ok(Self::Vaccines),
            526 => Ok(Self::Parts),
            529 => Ok(Self::AutomotiveHardware),
            530 => Ok(Self::Hardware),
            531 => Ok(Self::Plumbing),
            532 => Ok(Self::Electrical),
            533 => Ok(Self::Tools),
            534 => Ok(Self::Paint),
            535 => Ok(Self::Housewares),
            536 => Ok(Self::BuildingProducts),
            537 => Ok(Self::WeldingSupplies),
            538 => Ok(Self::Fasteners),
            539 => Ok(Self::OutdoorLivingAndAutomotive),
            540 => Ok(Self::Gifts),
            541 => Ok(Self::FloralArrangement),
            542 => Ok(Self::GiftCertificates),
            550 => Ok(Self::Services),
            551 => Ok(Self::OtherInventory),
            560 => Ok(Self::CommissionSales),
            576 => Ok(Self::AccountsReceivableDiscounts),
            577 => Ok(Self::MiscIncome),
            585 => Ok(Self::NorthrupKingCommission),
            586 => Ok(Self::InsuranceReimbursement),
            596 => Ok(Self::Discounts),
            597 => Ok(Self::ManufacturersCoupon),
            598 => Ok(Self::Adjustments),
            599 => Ok(Self::GrossIncome),
            _ => Err(AbcParseError::Custom(format!(
                "{value} does not map to a known ABC sales General Ledger"
            ))),
        }
    }
}

impl TryFrom<&str> for SaleLedger {
    type Error = AbcParseError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let int_value: usize = value
            .parse()
            .map_err(|e| AbcParseError::Custom(format!("{:?}", e)))?;
        SaleLedger::try_from(int_value)
    }
}

/// Attempt to convert a string into a [`BigDecimal`] by stripping out any characters that are not
/// digits or the decimal point. Used primarily to parse pricing from the csv ABC database export
///
/// # Arguments
/// * `price_str` - The string value to convert to a [`BigDecimal`]. This will primarily come from
/// fields in the database export such as cost or list
///
/// # Returns
/// A [`BigDecimal`] representing the number value passed in `price_str`
///
/// # Errors
/// [`bigdecimal::ParseBigDecimalError`] if `price_str` cannot be parsed into a [`BigDecimal`]
fn price_from_str(price_str: &str) -> Result<BigDecimal, bigdecimal::ParseBigDecimalError> {
    let price_str: String = price_str
        .chars()
        .filter(|c| c.is_digit(10) || c == &'.')
        .collect();
    price_str.parse()
}

/// Represents a product or inventory item in ABC accounting software.
///
/// # Example
/// ```rust
/// use abc_product::{AbcProduct, AbcProductsBySku, AbcParseError, SaleLedger};
/// use bigdecimal::BigDecimal;
///
/// // Manually creating an [`AbcProduct`]
/// let p = AbcProduct::new()
///     .with_sku("abc-123")
///     .with_desc("Test product")
///     .with_list("1.99".parse().unwrap())
///     .with_cost("0.99".parse().unwrap())
///     .with_stock(1.0)
///     .with_sale_gl(SaleLedger::Paint)
///     .build()
///     .unwrap();
///
/// // Creating a map of skus to their products
/// let products_by_sku: Result<AbcProductsBySku, AbcParseError> = AbcProduct::from_db_export("./item.data", "./item_posted.data");
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct AbcProduct {
    sku: String,
    desc: String,
    upcs: Vec<Gtin>,
    list: BigDecimal,
    cost: BigDecimal,
    stock: f64,
    group: Option<String>,
    weight: Option<f64>,
    last_sold: Option<chrono::NaiveDate>,
    alt_skus: Vec<String>,
    sale_gl: SaleLedger,
}

/// Used to safely construct an [`AbcProduct`]
pub struct AbcProductBuilder {
    sku: Option<String>,
    desc: Option<String>,
    upcs: Vec<Gtin>,
    list: Option<BigDecimal>,
    cost: Option<BigDecimal>,
    stock: Option<f64>,
    weight: Option<f64>,
    group: Option<String>,
    last_sold: Option<chrono::NaiveDate>,
    alt_skus: Vec<String>,
    sale_gl: Option<SaleLedger>,
}

/// A map where the key is a product's sku, and the value is the referenced [`AbcProduct`]
pub type AbcProductsBySku = HashMap<String, AbcProduct>;

#[derive(Debug)]
pub enum AbcParseError {
    /// An error caused by the csv parser.
    CsvError(csv::Error),
    /// A field required by [`AbcProduct`] is missing from the csv file. Value 0 is the name of the
    /// field that is missing. Value 1 is the row of the file that failed
    MissingField(String, usize),
    /// Attempted to combine data from the `item.data` and `item_posted.data` file under one
    /// [`AbcProduct`], but skus do not match
    MisMatchedSkus,
    /// Covers any additional errors that arise while parsing. Value 0 should be used to provide
    /// context to the error such as the row that the error occurred on
    Custom(String),
}

/// Just the fields that can be parsed from the `item_posted.data` file. Intended to be combined
/// with [`IntermediateProduct`] to create a full [`AbcProduct`]
struct IntermediatePostedProduct {
    sku: String,
    stock: f64,
    last_sold: Option<chrono::NaiveDate>,
}

/// Just the fields that can be parsed from the `item.data` file. Intended to be combined with
/// [`IntermediatePostedProduct`] to create a full [`AbcProduct`]
struct IntermediateBaseProduct {
    sku: String,
    desc: String,
    upcs: Vec<Gtin>,
    list: BigDecimal,
    cost: BigDecimal,
    group: Option<String>,
    weight: Option<f64>,
    alt_skus: Vec<String>,
    sale_gl: SaleLedger,
}

impl AbcProduct {
    /// Create a new instance of [`AbcProductBuilder`] with all values set to [`None`] by default
    pub fn new() -> AbcProductBuilder {
        AbcProductBuilder::new()
    }

    /// Fetch this product's sku
    pub fn sku(&self) -> String {
        self.sku.clone()
    }

    /// Fetch this product's description
    pub fn desc(&self) -> String {
        self.desc.clone()
    }

    /// Fetch the list of this product's [`Gtin`]s (UPCs)
    pub fn upcs(&self) -> Vec<Gtin> {
        self.upcs.to_vec()
    }

    /// Fetch this product's list price as a [`BigDecimal`]
    pub fn list(&self) -> BigDecimal {
        self.list.clone()
    }

    /// Fetch this product's cost as a [`BigDecimal`]
    pub fn cost(&self) -> BigDecimal {
        self.cost.clone()
    }

    /// Fetch this product's current inventory level or stock
    pub fn stock(&self) -> f64 {
        self.stock
    }

    /// How much does the product weigh in pounds. [`None`] if no weight is provided
    pub fn weight(&self) -> Option<f64> {
        self.weight
    }

    /// What product group does this product belong to? Should be a single character from A-Z or
    /// [`None`]
    pub fn group(&self) -> Option<String> {
        self.group.to_owned()
    }

    /// The date that this product was last sold. [`None`] if the product has not been sold
    pub fn last_sold(&self) -> Option<chrono::NaiveDate> {
        self.last_sold
    }

    /// The list of alternative skus for this product
    pub fn alt_skus(&self) -> Vec<String> {
        self.alt_skus.to_owned()
    }

    /// The general ledger used to account for the sale
    pub fn sale_gl(&self) -> SaleLedger {
        self.sale_gl.clone()
    }

    /// Create a map of skus to [`AbcProduct`]s by parsing ABC database export files.
    ///
    /// In order to run a database export, run report 7-10, select "I" (Inventory) as the file to export. All
    /// other parameters can be skipped or left as default. Run the report to the Screen. After a
    /// few seconds, two files should be created at
    /// C:\ABC Software\Database Export\Company001\Data\item.data and
    /// C:\ABC Software\Database Export\Company001\Data\item_posted.data.
    ///
    /// # Arguments
    /// * `item_path` - The path to the item.data file generated by the db export. This will
    /// probably be C:\ABC Software\Database Export\Company001\Data\item.data.
    /// * `item_posted_path` - The path to the item_posted.data file generated by the db export.
    /// This will probably be C:\ABC Software\Database Export\Company001\Data\item_posted.data
    ///
    /// # Returns
    /// A [`HashMap`] of ABC SKUs to the [`AbcProduct`] they belong to
    ///
    /// # Errors
    /// The data files are long, and ABC does not always produce them correctly. Therefore, if any
    /// required fields are missing or if certain numeric values (prices, weight) cannot be parsed,
    /// then an [`AbcParseError`] will be returned
    pub fn from_db_export(
        item_path: &str,
        item_posted_path: &str,
    ) -> Result<AbcProductsBySku, AbcParseError> {
        let base_products = IntermediateBaseProduct::parse_item_data(item_path)?;
        let posted_products = IntermediatePostedProduct::parse_item_posted_data(item_posted_path)?;
        if base_products.len() != posted_products.len() {
            return Err(AbcParseError::Custom(
                "The item_posted.data and item.data files have a different nember of items"
                    .to_string(),
            ));
        }

        let mut products = AbcProductsBySku::new();
        for (sku, base_product) in base_products {
            let posted_product =
                posted_products
                    .get(&sku)
                    .ok_or(AbcParseError::Custom(format!(
                        "item_posted.data file has no product with sku '{}'",
                        sku
                    )))?;
            products.insert(sku, AbcProduct::try_from((&base_product, posted_product))?);
        }
        Ok(products)
    }
}

impl TryFrom<(&IntermediateBaseProduct, &IntermediatePostedProduct)> for AbcProduct {
    type Error = AbcParseError;

    fn try_from(
        (inter, posted): (&IntermediateBaseProduct, &IntermediatePostedProduct),
    ) -> Result<Self, Self::Error> {
        if inter.sku != posted.sku {
            return Err(AbcParseError::MisMatchedSkus);
        }
        Ok(AbcProduct {
            sku: inter.sku.to_string(),
            desc: inter.desc.to_string(),
            alt_skus: inter.alt_skus.to_vec(),
            upcs: inter.upcs.to_vec(),
            cost: inter.cost.clone(),
            list: inter.list.clone(),
            group: inter.group.clone(),
            weight: inter.weight,
            stock: posted.stock,
            last_sold: posted.last_sold,
            sale_gl: inter.sale_gl.clone(),
        })
    }
}

impl AbcProductBuilder {
    /// Create a new instance of [`AbcProductBuilder`] with all values set to [`None`] by default
    pub fn new() -> Self {
        AbcProductBuilder {
            sku: None,
            desc: None,
            upcs: Vec::new(),
            list: None,
            cost: None,
            stock: None,
            weight: None,
            group: None,
            last_sold: None,
            alt_skus: Vec::new(),
            sale_gl: None,
        }
    }

    /// Set the sku for this product
    pub fn with_sku(self, sku: &str) -> Self {
        AbcProductBuilder {
            sku: Some(sku.to_string()),
            ..self
        }
    }

    /// Set the description for this product
    pub fn with_desc(self, desc: &str) -> Self {
        AbcProductBuilder {
            desc: Some(desc.to_string()),
            ..self
        }
    }

    /// Set the value of the list of UPCs for this product
    pub fn with_upcs(self, upcs: Vec<Gtin>) -> Self {
        AbcProductBuilder { upcs, ..self }
    }

    /// Add a UPC to the list of UPCs for this product
    pub fn add_upc(self, upc: Gtin) -> Self {
        let mut new_upcs = self.upcs.to_vec();
        new_upcs.push(upc);
        AbcProductBuilder {
            upcs: new_upcs,
            ..self
        }
    }

    /// Set this product's list price
    pub fn with_list(self, list: BigDecimal) -> Self {
        AbcProductBuilder {
            list: Some(list),
            ..self
        }
    }

    /// Set this product's cost
    pub fn with_cost(self, cost: BigDecimal) -> Self {
        AbcProductBuilder {
            cost: Some(cost),
            ..self
        }
    }

    /// Set the stock level (inventory) of this product
    pub fn with_stock(self, stock: f64) -> Self {
        AbcProductBuilder {
            stock: Some(stock),
            ..self
        }
    }

    /// Set this product's weight in pounds
    pub fn with_weight(self, weight: f64) -> Self {
        AbcProductBuilder {
            weight: Some(weight),
            ..self
        }
    }

    /// This product's group. Should be a character from A-Z
    ///
    /// # Arguments
    /// * `group` - A character between 'A' and 'Z' inclusive that describes the discount group
    ///
    /// # Returns
    /// If `group` is a character between 'A' and 'Z' inclusive, return Some([`AbcProductBuilder`])
    /// with a group of `group`. If `group` is outside of the range 'A' to 'Z', return [`None`]
    pub fn with_group(self, group: char) -> Option<Self> {
        if (group < 'A' || group > 'Z') && (group < 'a' || group > 'z') {
            return None;
        }
        Some(AbcProductBuilder {
            group: Some(group.to_string().to_uppercase()),
            ..self
        })
    }

    /// Sets the date that this product was last sold
    pub fn with_last_sold(self, last_sold: NaiveDate) -> Self {
        AbcProductBuilder {
            last_sold: Some(last_sold),
            ..self
        }
    }

    /// Sets the value of all alternative skus for this builder
    pub fn with_alt_skus(self, alt_skus: &[String]) -> Self {
        AbcProductBuilder {
            alt_skus: alt_skus.to_vec(),
            ..self
        }
    }

    /// Add a single alternative sku to the list of alternative skus for this builder
    pub fn add_alt_sku(self, alt: impl ToString) -> Self {
        let mut new_skus = self.alt_skus;
        new_skus.push(alt.to_string());
        Self {
            alt_skus: new_skus,
            ..self
        }
    }

    /// Add a sale general ledger record to this builder
    pub fn with_sale_gl(self, ledger: impl Into<SaleLedger>) -> Self {
        let sale_gl: SaleLedger = ledger.into();
        Self {
            sale_gl: Some(sale_gl),
            ..self
        }
    }

    /// Attempt to construct an [`AbcProduct`] from this builder
    ///
    /// # Returns
    /// Some([`AbcProduct`]) if the following required fields have been supplied:
    /// - sku
    /// - desc
    /// - list
    /// - cost
    /// - stock
    ///
    /// If at least one of the required fields is missing, then return [`None`]
    pub fn build(self) -> Result<AbcProduct, AbcParseError> {
        Ok(AbcProduct {
            sku: self
                .sku
                .clone()
                .ok_or(AbcParseError::MissingField("sku".to_string(), 0))?,
            desc: self
                .desc
                .clone()
                .ok_or(AbcParseError::MissingField("desc".to_string(), 0))?,
            upcs: self.upcs,
            list: self
                .list
                .ok_or(AbcParseError::MissingField("list".to_string(), 0))?,
            cost: self
                .cost
                .ok_or(AbcParseError::MissingField("cost".to_string(), 0))?,
            stock: self
                .stock
                .ok_or(AbcParseError::MissingField("stock".to_string(), 0))?,
            weight: self.weight,
            group: self.group,
            last_sold: self.last_sold,
            alt_skus: self.alt_skus,
            sale_gl: self
                .sale_gl
                .ok_or(AbcParseError::MissingField("sale_gl".to_string(), 0))?,
        })
    }
}

impl From<AbcProduct> for AbcProductBuilder {
    fn from(value: AbcProduct) -> Self {
        AbcProductBuilder {
            sku: Some(value.sku()),
            desc: Some(value.desc()),
            upcs: value.upcs(),
            list: Some(value.list),
            cost: Some(value.cost),
            stock: Some(value.stock),
            weight: value.weight,
            group: value.group,
            last_sold: value.last_sold,
            alt_skus: value.alt_skus,
            sale_gl: Some(value.sale_gl),
        }
    }
}

impl std::fmt::Display for AbcParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingField(field, row) => {
                write!(f, "Missing field `{}` in row {}", field, row)
            }
            Self::MisMatchedSkus => {
                write!(
                    f,
                    "Attempted to combine data from `item.data` and `item_posted.data` into a single [`AbcProduct`], but the skus do not match"
                )
            }
            _ => write!(f, "{:?}", self),
        }
    }
}

impl std::error::Error for AbcParseError {}

impl From<csv::Error> for AbcParseError {
    fn from(value: csv::Error) -> Self {
        Self::CsvError(value)
    }
}

impl IntermediatePostedProduct {
    /// Create an intermediate map of skus to [`AbcProduct`] by parsing just the `item_posted.data`
    /// file
    ///
    /// # Arguments
    /// * `item_posted_path` - The path to the `item_posted.data` file that contains posted data
    /// fields for ABC inventory items
    ///
    /// # Returns
    /// A map from skus to [`IntermediatePostedProduct`]. Each [`IntermediatePostedProduct`]
    /// contains only the data for an [`AbcProduct`] that can be parsed from `item_posted.data`
    ///
    /// # Errors
    /// Most errors will be related to parsing the csv file. There is also potential for
    /// [`AbcParseError`]s to be raised if there are missing fields or other problems
    /// deserializing the data
    fn parse_item_posted_data(
        item_posted_path: &str,
    ) -> Result<HashMap<String, IntermediatePostedProduct>, AbcParseError> {
        let mut posted_data = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .from_path(item_posted_path)?;

        let mut products = HashMap::new();
        let mut i = 0;
        while let Some(row) = posted_data.records().next() {
            i += 1;
            let row = row?;
            let sku = row
                .get(0)
                .ok_or(AbcParseError::MissingField("sku".to_string(), i))?
                .to_string();
            let stock_str = row
                .get(19)
                .ok_or(AbcParseError::MissingField("stock".to_string(), i))?
                .to_string();
            let stock: f64 = stock_str.parse().or(Err(AbcParseError::Custom(format!(
                "Cannot parse f64 from stock_str in row {} of posted items",
                i
            ))))?;
            let last_sold_str: String = row
                .get(1)
                .ok_or(AbcParseError::MissingField("last_sold".to_string(), i))?
                .to_string();
            let last_sold = chrono::NaiveDate::parse_from_str(&last_sold_str, "%Y-%m-%d").ok();
            products.insert(
                sku.clone(),
                IntermediatePostedProduct {
                    sku,
                    stock,
                    last_sold,
                },
            );
        }
        Ok(products)
    }
}

impl IntermediateBaseProduct {
    /// Parses the `item.data` file to produce an intermediate mapping from skus to partial
    /// [`AbcProduct`] data. A full [`AbcProduct`] can be derived by combining [`IntermediateBaseProduct`]s
    /// with [`IntermediatePostedProduct`]s that share a sku.
    ///
    /// # Arguments
    /// * `item_path` - The path to the ABC db export file usually called `item.data`. This file
    /// contains most of the information for each inventory item
    ///
    /// # Returns
    /// A map from skus to [`IntermediateBaseProduct`]. Each [`IntermediateBaseProduct`]
    /// contains only the data for an [`AbcProduct`] that can be parsed from `item.data`
    ///
    /// # Errors
    /// Most errors will be related to parsing the csv file. There is also potential for
    /// [`AbcParseError`]s to be raised if there are missing fields or other problems
    /// deserializing the data
    fn parse_item_data(
        item_path: &str,
    ) -> Result<HashMap<String, IntermediateBaseProduct>, AbcParseError> {
        let mut item_data = csv::ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(false)
            .from_path(item_path)?;

        let mut i = 0;
        let mut products = HashMap::new();
        while let Some(row) = item_data.records().next() {
            i += 1;
            let row = row?;
            let sku = row
                .get(0)
                .ok_or(AbcParseError::MissingField("sku".to_string(), i))?
                .to_string();
            let desc = row
                .get(1)
                .ok_or(AbcParseError::MissingField("desc".to_string(), i))?
                .to_string();
            let upc_str: String = row
                .get(43)
                .ok_or(AbcParseError::MissingField("upcs".to_string(), i))?
                .chars()
                .filter(|c| c.is_digit(10) || *c == ',')
                .collect();
            let upcs: Vec<Gtin> = upc_str
                .split(",")
                .filter_map(|s| {
                    if s.len() == 11 {
                        // Some ABC UPCs leave out the check digit, so make one up and let [`Gtin::from_str_nonstrict`] fix it
                        Some(Gtin::nonstrict_new(&format!("{}0", s)))
                    } else if s.len() < 11 {
                        // Anything less than 11 characters long is probably a dead upc
                        None
                    } else {
                        // Anything 12 characters and up has a chance of being a good upc
                        Some(Gtin::nonstrict_new(s))
                    }
                })
                .collect();
            let list = row
                .get(6)
                .ok_or(AbcParseError::MissingField("list".to_string(), i))?;
            let list = price_from_str(list).or(Err(AbcParseError::Custom(format!(
                "Cannot parse a price for list in row {}",
                i
            ))))?;
            let cost = row
                .get(8)
                .ok_or(AbcParseError::MissingField("cost".to_string(), i))?;
            let cost = price_from_str(cost).or(Err(AbcParseError::Custom(format!(
                "Cannot parse a price for cost in row {}",
                i
            ))))?;
            let weight_str = row
                .get(45)
                .ok_or(AbcParseError::MissingField("weight".to_string(), i))?;
            let weight = match weight_str.parse::<f64>() {
                Ok(f) => Some(f),
                Err(_) => None,
            };
            let group = row.get(18);
            let group = match group {
                Some(g) => {
                    if g.is_empty() {
                        None
                    } else {
                        Some(g.to_owned())
                    }
                }
                None => None,
            };
            let alt_skus = [row.get(40), row.get(41), row.get(42)]
                .iter()
                .filter_map(|o| match o {
                    Some("") => None,
                    Some(s) => Some(s.to_string()),
                    None => None,
                })
                .collect();
            let sale_gl: SaleLedger = row
                .get(2)
                .ok_or(AbcParseError::MissingField("sale_gl".to_string(), i))?
                .try_into()?;
            products.insert(
                sku.clone(),
                IntermediateBaseProduct {
                    sku,
                    desc,
                    upcs,
                    list,
                    cost,
                    weight,
                    group,
                    alt_skus,
                    sale_gl,
                },
            );
        }
        Ok(products)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parser() {
        let item_path = "./item.data";
        let item_posted_path = "./item_posted.data";
        let products = AbcProduct::from_db_export(item_path, item_posted_path).unwrap();
        assert_eq!(
            products,
            AbcProductsBySku::from([
                (
                    "123456".to_string(),
                    AbcProduct::new()
                        .with_sku("123456")
                        .with_desc("PRODUCT A")
                        .add_upc(Gtin::nonstrict_new("85875500014"))
                        .with_cost("1.23".parse().unwrap())
                        .with_stock(0.00)
                        .with_list("5.99".parse().unwrap())
                        .with_last_sold(NaiveDate::from_str("2024-11-16").unwrap())
                        .add_alt_sku("ALT")
                        .with_sale_gl(SaleLedger::Paint)
                        .build()
                        .unwrap()
                ),
                (
                    "ABC123".to_string(),
                    AbcProduct::new()
                        .with_sku("ABC123")
                        .with_desc("PRODUCT B")
                        .with_stock(-6.0)
                        .with_list("8.12".parse().unwrap())
                        .with_cost("5.23".parse().unwrap())
                        .add_alt_sku("ALT SKU")
                        .with_group('A')
                        .unwrap()
                        .with_last_sold("2019-05-28".parse().unwrap())
                        .with_sale_gl(SaleLedger::LawnAndGarden)
                        .build()
                        .unwrap()
                )
            ])
        );
    }
}
