use polars::prelude::{LazyCsvReader, LazyFileListReader, LazyFrame, PolarsError};
use reqwest::ClientBuilder;
use std::io::{Cursor, Read};
use std::path::Path;
use tokio::fs;
use tokio::task::JoinSet;
use zip::ZipArchive;

const MICRODATA_PREFIX: &str = "MICRODADOS_CADASTRO";

#[derive(Debug, Clone, Copy)]
pub struct Ces {
    year: u16,
}

impl Ces {
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
}

impl Ces {
    pub fn lf_cursos(&self) -> Result<LazyFrame, PolarsError> {
        LazyCsvReader::new(&self.path(Microdata::Cursos))
            .with_delimiter(b';')
            .finish()
    }
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
                log::error!("The data for one year failed: {}", e);
                e
            })
            .unwrap();
        }

        log::info!("All files are ok!");

        Ok(())
    }

    /// Ensures that input data for this Ces is downloaded.
    pub async fn ensure_data(&self) -> anyhow::Result<()> {
        if self.already_downloaded().await? {
            log::debug!("[{}] data already exists.", self.year());
        } else {
            log::debug!("[{}]", self.year());
            self.download_data().await?;
        }
        Ok(())
    }

    async fn already_downloaded(&self) -> anyhow::Result<bool> {
        let path_string = self.path(Microdata::Cursos);
        Ok(Path::new(&path_string).try_exists()?)
    }

    fn path(&self, kind: Microdata) -> String {
        match kind {
            Microdata::Cursos => format!("input/cursos.{}.csv", self.year()),
        }
    }

    async fn download_data(self) -> anyhow::Result<()> {
        let zip = self.zip().await?;

        log::debug!("[{}] extracting zip files...", self.year());
        let cursos =
            tokio::task::spawn_blocking(move || self.extract_cursos_microdata(zip).unwrap())
                .await?;
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

    async fn zip(&self) -> reqwest::Result<Vec<u8>> {
        // TODO: find a way to store the server's certificate here, because
        // it seems that when downloading the file it doesn't send the certificate
        log::debug!("[{}] Sending request to {}", self.year, self.url());
        let client = ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .build()?;
        Ok(client.get(self.url()).send().await?.bytes().await?.to_vec())
    }

    fn extract_cursos_microdata(&self, zip: Vec<u8>) -> anyhow::Result<String> {
        log::trace!("extract_cursos_microdata");
        let mut archive = ZipArchive::new(Cursor::new(zip))?;
        let names: Vec<String> = archive
            .file_names()
            .filter(|name| name.contains(MICRODATA_PREFIX))
            .filter(|name| name.contains(Microdata::Cursos.name()))
            .map(String::from)
            .collect();

        for name in names {
            let mut zip_file = archive
                .by_name(&name)
                .expect("Using name provided by `file_names`");
            let mut bytes = Vec::new();
            zip_file.read_to_end(&mut bytes)?;

            let clone = bytes.clone();
            let downloaded_md5 = format!("{:x}", md5::compute(clone));
            let correct_md5 = Microdata::Cursos.original_md5(self.year()).unwrap();
            if downloaded_md5 != correct_md5 {
                dbg!(self.year(), downloaded_md5, correct_md5);
                anyhow::bail!("[{}], wrong md5", self.year());
            } else {
                log::debug!("[{}] correct md5", self.year());
            }

            let string = iso_8559_1_to_utf8(bytes);
            return Ok(string);
        }
        anyhow::bail!("Cursos microdata wasn't found");
    }

    async fn save_cursos(&self, s: String) -> std::io::Result<()> {
        let input = Path::new("./input");
        let filename = format!("cursos.{}.csv", self.year());
        fs::create_dir_all(&input).await?;
        fs::write(&input.join(filename), s).await
    }
}

/// Converts a file encoded in ISO-8559-1 to UTF-8 encoding,
/// which is the encoding of Rust strings.
fn iso_8559_1_to_utf8(bytes: Vec<u8>) -> String {
    bytes.iter().map(|&c| c as char).collect()
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

    fn original_md5(&self, year: u16) -> Option<&str> {
        match self {
            Self::Cursos => match year {
                2009 => Some("677421fb8ad9442370175cbadae05b77"),
                2010 => Some("8ea106ef7dc41a27a43b9f246cfd3ffd"),
                2011 => Some("f626dd6d17e8f31f78ddf90f680ace48"),
                2012 => Some("f896c4a4e2b10adcf846d91486ab0ce8"),
                2013 => Some("2bbfbe1a9afe1fe5d0d7384901ae3b7e"),
                2014 => Some("bf70eb93a2a5cce0e0a48295c4834c20"),
                2015 => Some("b5bd1b6b10b4f66f359deed4ac48cb80"),
                2016 => Some("a9475f5f6815a5befb8bc91b8e2c7b1c"),
                2017 => Some("af97168b2d83b0e4b6c1572e619c183b"),
                2018 => Some("b852881daa9328e4ff3f3a2c6115ba51"),
                2019 => Some("f80ea1eddafae4780728e6fb26aa549f"),
                2020 => Some("a84c1efeedd8bcec4848ec8217b92b98"),
                2021 => Some("05d78ff911cea316cd65f08b0e93e83d"),
                _ => None,
            },
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
