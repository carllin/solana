use crate::{
    hash::Hash,
    instruction::InstructionError,
    {clock::Epoch, pubkey::Pubkey},
};
use std::{
    cell::{Ref, RefCell, RefMut},
    cmp, fmt,
    iter::FromIterator,
    rc::Rc,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// An Account with data that is stored on chain
#[repr(C)]
#[derive(Serialize, Deserialize, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct Account<T>
where
    T: Serialize + DeserializeOwned,
{
    /// lamports in the account
    pub lamports: u64,
    /// data held in this account
    #[serde(with = "serde_bytes")]
    pub data: Vec<u8>,
    /// the program that owns this account. If executable, the program that loads this account.
    pub owner: Pubkey,
    /// this account's data contains a loaded program (and is now read-only)
    pub executable: bool,
    /// the epoch at which this account will next owe rent
    pub rent_epoch: Epoch,
    /// Hash of this account's state, skip serializing as to not expose to external api
    /// Used for keeping the accounts state hash updated.
    #[serde(skip)]
    pub hash: Hash,
}

/// skip comparison of account.hash, since it is only meaningful when the account is loaded in a
/// given fork and some tests do not have that.
impl<T: Serialize + DeserializeOwned> PartialEq for Account<T> {
    fn eq(&self, other: &Self) -> bool {
        self.lamports == other.lamports
            && self.data == other.data
            && self.owner == other.owner
            && self.executable == other.executable
            && self.rent_epoch == other.rent_epoch
    }
}

impl<T: Serialize + DeserializeOwned> Eq for Account<T> {}

impl<T: Serialize + DeserializeOwned> fmt::Debug for Account<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let data_len = cmp::min(64, self.data.len());
        let data_str = if data_len > 0 {
            format!(" data: {}", hex::encode(self.data[..data_len].to_vec()))
        } else {
            "".to_string()
        };
        write!(
            f,
            "Account {{ lamports: {} data.len: {} owner: {} executable: {} rent_epoch: {}{} hash: {} }}",
            self.lamports,
            self.data.len(),
            self.owner,
            self.executable,
            self.rent_epoch,
            data_str,
            self.hash,
        )
    }
}

impl<T: Serialize + DeserializeOwned> Account<T> {
    pub fn new(lamports: u64, space: usize, owner: &Pubkey) -> Self {
        Self {
            lamports,
            data: vec![0u8; space],
            owner: *owner,
            ..Self::default()
        }
    }
    pub fn new_ref(lamports: u64, space: usize, owner: &Pubkey) -> Rc<RefCell<Self>> {
        Rc::new(RefCell::new(Self::new(lamports, space, owner)))
    }

    pub fn new_data(lamports: u64, state: &T, owner: &Pubkey) -> Result<Self, bincode::Error> {
        let data = bincode::serialize(state)?;
        Ok(Self {
            lamports,
            data,
            owner: *owner,
            ..Self::default()
        })
    }
    pub fn new_ref_data(
        lamports: u64,
        state: &T,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        Ok(RefCell::new(Self::new_data(lamports, state, owner)?))
    }

    pub fn new_data_with_space(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<Self, bincode::Error> {
        let mut account = Self::new(lamports, space, owner);

        account.serialize_data(state)?;

        Ok(account)
    }
    pub fn new_ref_data_with_space(
        lamports: u64,
        state: &T,
        space: usize,
        owner: &Pubkey,
    ) -> Result<RefCell<Self>, bincode::Error> {
        Ok(RefCell::new(Self::new_data_with_space(
            lamports, state, space, owner,
        )?))
    }

    pub fn deserialize_data(&self) -> Result<T, bincode::Error> {
        bincode::deserialize(&self.data)
    }

    pub fn serialize_data(&mut self, state: &T) -> Result<(), bincode::Error> {
        if bincode::serialized_size(state)? > self.data.len() as u64 {
            return Err(Box::new(bincode::ErrorKind::SizeLimit));
        }
        bincode::serialize_into(&mut self.data[..], state)
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct KeyedAccount<'a, T: Serialize + DeserializeOwned> {
    is_signer: bool, // Transaction was signed by this account's key
    is_writable: bool,
    key: &'a Pubkey,
    pub account: &'a RefCell<Account<T>>,
}

impl<'a, T: Serialize + DeserializeOwned> KeyedAccount<'a, T> {
    pub fn signer_key(&self) -> Option<&Pubkey> {
        if self.is_signer {
            Some(self.key)
        } else {
            None
        }
    }

    pub fn unsigned_key(&self) -> &Pubkey {
        self.key
    }

    pub fn is_writable(&self) -> bool {
        self.is_writable
    }

    pub fn lamports(&self) -> Result<u64, InstructionError> {
        Ok(self.try_borrow()?.lamports)
    }

    pub fn data_len(&self) -> Result<usize, InstructionError> {
        Ok(self.try_borrow()?.data.len())
    }

    pub fn data_is_empty(&self) -> Result<bool, InstructionError> {
        Ok(self.try_borrow()?.data.is_empty())
    }

    pub fn owner(&self) -> Result<Pubkey, InstructionError> {
        Ok(self.try_borrow()?.owner)
    }

    pub fn executable(&self) -> Result<bool, InstructionError> {
        Ok(self.try_borrow()?.executable)
    }

    pub fn try_account_ref(&'a self) -> Result<Ref<Account>, InstructionError> {
        self.try_borrow()
    }

    pub fn try_account_ref_mut(&'a self) -> Result<RefMut<Account>, InstructionError> {
        self.try_borrow_mut()
    }

    fn try_borrow(&self) -> Result<Ref<Account>, InstructionError> {
        self.account
            .try_borrow()
            .map_err(|_| InstructionError::AccountBorrowFailed)
    }
    fn try_borrow_mut(&self) -> Result<RefMut<Account>, InstructionError> {
        self.account
            .try_borrow_mut()
            .map_err(|_| InstructionError::AccountBorrowFailed)
    }

    pub fn new(key: &'a Pubkey, is_signer: bool, account: &'a RefCell<Account>) -> Self {
        Self {
            is_signer,
            is_writable: true,
            key,
            account,
        }
    }

    pub fn new_readonly(key: &'a Pubkey, is_signer: bool, account: &'a RefCell<Account>) -> Self {
        Self {
            is_signer,
            is_writable: false,
            key,
            account,
        }
    }
}

impl<'a, T: Serialize + DeserializeOwned> PartialEq for KeyedAccount<'a, T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<'a, T: Serialize + DeserializeOwned> From<(&'a Pubkey, &'a RefCell<Account<T>>)>
    for KeyedAccount<'a, T>
{
    fn from((key, account): (&'a Pubkey, &'a RefCell<Account>)) -> Self {
        Self {
            is_signer: false,
            is_writable: true,
            key,
            account,
        }
    }
}

impl<'a, T: Serialize + DeserializeOwned> From<(&'a Pubkey, bool, &'a RefCell<Account<T>>)>
    for KeyedAccount<'a, T>
{
    fn from((key, is_signer, account): (&'a Pubkey, bool, &'a RefCell<Account>)) -> Self {
        Self {
            is_signer,
            is_writable: true,
            key,
            account,
        }
    }
}

impl<'a, T: Serialize + DeserializeOwned> From<&'a (&'a Pubkey, &'a RefCell<Account<T>>)>
    for KeyedAccount<'a, T>
{
    fn from((key, account): &'a (&'a Pubkey, &'a RefCell<Account>)) -> Self {
        Self {
            is_signer: false,
            is_writable: true,
            key,
            account,
        }
    }
}

pub fn create_keyed_accounts<'a, T: Serialize + DeserializeOwned>(
    accounts: &'a [(&'a Pubkey, &'a RefCell<Account<T>>)],
) -> Vec<KeyedAccount<'a, T>> {
    accounts.iter().map(Into::into).collect()
}

pub fn create_keyed_is_signer_accounts<'a, T: Serialize + DeserializeOwned>(
    accounts: &'a [(&'a Pubkey, bool, &'a RefCell<Account<T>>)],
) -> Vec<KeyedAccount<'a, T>> {
    accounts
        .iter()
        .map(|(key, is_signer, account)| KeyedAccount {
            is_signer: *is_signer,
            is_writable: false,
            key,
            account,
        })
        .collect()
}

pub fn create_keyed_readonly_accounts<T: Serialize + DeserializeOwned>(
    accounts: &[(Pubkey, RefCell<Account<T>>)],
) -> Vec<KeyedAccount<T>> {
    accounts
        .iter()
        .map(|(key, account)| KeyedAccount {
            is_signer: false,
            is_writable: false,
            key,
            account,
        })
        .collect()
}

/// Return all the signers from a set of KeyedAccounts
pub fn get_signers<A>(keyed_accounts: &[KeyedAccount]) -> A
where
    A: FromIterator<Pubkey>,
{
    keyed_accounts
        .iter()
        .filter_map(|keyed_account| keyed_account.signer_key())
        .cloned()
        .collect::<A>()
}
