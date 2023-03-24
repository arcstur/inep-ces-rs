use reqwest::ClientBuilder;
use std::io::{Cursor, Read};
use tokio::fs;
use tokio::task::JoinSet;
use zip::ZipArchive;

const MICRODATA_PREFIX: &str = "MICRODADOS_CADASTRO_";

pub struct Ces {
    year: u16,
}

impl Ces {
    /// Ensures that all input data is downloaded.
    pub async fn ensure_all_data() -> anyhow::Result<()> {
        let mut set = JoinSet::new();

        for year in 2009..2022 {
            set.spawn(async move {
                Ces::new(year).ensure_data().await.unwrap();
            });
        }

        while let Some(res) = set.join_next().await {
            res.map_err(|e| {
                log::error!("The data download for one year failed: {}", e);
                e
            })
            .unwrap();
        }

        log::info!("All files were downloaded successfully!");

        Ok(())
    }

    /// Get a `Vec` of all `Ces` available.
    pub fn all() -> Vec<Ces> {
        (2009..2022).map(|year| Ces::new(year)).collect()
    }

    /// Creates a new object holding a
    /// Censo da Educação Superior information for a given year.
    ///
    /// # Panics
    /// Panic if input `year` is less than 2009, given that before this year
    /// the data has a different structure.
    pub fn new(year: u16) -> Ces {
        if year < 2009 {
            panic!("Years before 2009 are not suppported in this software yet.")
        } else {
            Ces { year }
        }
    }

    pub fn year(&self) -> u16 {
        self.year
    }

    /// Ensures that input data for this Ces is downloaded.
    pub async fn ensure_data(&self) -> anyhow::Result<()> {
        let zip = self.zip().await?;

        log::debug!("[{}] extracting zip files...", self.year());
        let cursos =
            tokio::task::spawn_blocking(move || extract_cursos_microdata(zip).unwrap()).await?;
        self.save_cursos(cursos).await?;

        log::info!("[{}] data downloaded!", self.year());
        Ok(())
    }

    fn url(&self) -> String {
        format!(
            "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{}.zip",
            self.year
        )
    }

    pub async fn zip(&self) -> reqwest::Result<Vec<u8>> {
        // TODO: find a way to store the server's certificate here, because
        // it seems that when downloading the file it doesn't send the certificate
        log::debug!("[{}] Sending request to {}", self.year, self.url());
        let client = ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .build()?;
        Ok(client.get(self.url()).send().await?.bytes().await?.to_vec())
    }

    async fn save_cursos(&self, s: String) -> std::io::Result<()> {
        fs::write(format!("input/cursos.{}.csv", self.year()), s).await
    }
}

/// Converts a file encoded in ISO-8559-1 to UTF-8 encoding,
/// which is the encoding of Rust strings.
fn iso_8559_1_to_utf8(bytes: Vec<u8>) -> String {
    bytes.iter().map(|&c| c as char).collect()
}

fn extract_cursos_microdata(zip: Vec<u8>) -> anyhow::Result<String> {
    log::trace!("extract_cursos_microdata");
    let mut archive = ZipArchive::new(Cursor::new(zip))?;
    let names: Vec<String> = archive
        .file_names()
        .filter(|name| name.contains(MICRODATA_PREFIX))
        .map(String::from)
        .collect();
    for name in names {
        let mut zip_file = archive
            .by_name(&name)
            .expect("Using name provided by `file_names`");
        let mut bytes = Vec::new();
        zip_file.read_to_end(&mut bytes)?;
        let string = iso_8559_1_to_utf8(bytes);
        if string.contains(Microdata::Cursos.name()) {
            return Ok(string);
        }
    }
    anyhow::bail!("Cursos microdata wasn't found");
}

enum Microdata {
    Cursos,
}

impl Microdata {
    fn name(&self) -> &str {
        match self {
            Self::Cursos => "CURSOS",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn create_ces_with_year_before_2009() {
        let _ces = Ces::new(2008);
    }

    #[test]
    fn create_ces_with_year_after_2009() {
        for year in 2009..2022 {
            let _ces = Ces::new(year);
        }
    }

    #[test]
    fn url() {
        let ces = Ces::new(2011);
        assert_eq!(
            ces.url(),
            "https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_2011.zip",
        )
    }
}
