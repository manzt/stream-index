use anyhow::{Context, Result};
use noodles::{bam, csi, sam};
use object_store::{http, ObjectStore};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio_util::io::StreamReader;

fn is_coordinate_sorted(header: &sam::Header) -> bool {
    use sam::header::record::value::map::header::SortOrder;
    if let Some(hdr) = header.header() {
        if let Some(sort_order) = hdr.sort_order() {
            return sort_order == SortOrder::Coordinate;
        }
    }
    false
}

async fn build_bam_index<R: AsyncRead + Unpin>(reader: &mut R) -> Result<csi::Index> {
    let mut bam_reader = bam::AsyncReader::new(reader);
    let header: sam::Header = bam_reader.read_header().await?.parse()?;
    bam_reader.read_reference_sequences().await?; // idk, need to read this first
    if !is_coordinate_sorted(&header) {
        anyhow::bail!("BAM file is not coordinate sorted");
    }
    let mut start_position = bam_reader.virtual_position();
    let mut builder = csi::index::Indexer::default();
    let mut record = sam::alignment::Record::default();
    while bam_reader.read_record(&header, &mut record).await? != 0 {
        let end_position = bam_reader.virtual_position();
        let chunk = csi::index::reference_sequence::bin::Chunk::new(start_position, end_position);
        let alignment_context = match (
            record.reference_sequence_id(),
            record.alignment_start(),
            record.alignment_end(),
        ) {
            (Some(id), Some(start), Some(end)) => {
                Some((id, start, end, !record.flags().is_unmapped()))
            }
            _ => None,
        };
        builder.add_record(alignment_context, chunk)?;
        start_position = end_position;
    }
    let index = builder.build(header.reference_sequences().len());
    Ok(index)
}

async fn write_bam_index<W: AsyncWrite + Unpin>(writer: &mut W, index: &csi::Index) -> Result<()> {
    let mut writer = bam::bai::AsyncWriter::new(writer);
    writer.write_header().await?;
    writer.write_index(index).await?;
    Ok(())
}

async fn get_async_stream_reader(url: &url::Url) -> Result<impl AsyncRead + Unpin> {
    let (store, path) = match url.scheme() {
        "http" | "https" => {
            let path: object_store::path::Path = "".try_into().unwrap();
            let store = http::HttpBuilder::new().with_url(url.clone()).build()?;
            (store, path)
        }
        _ => {
            unimplemented!("Only HTTP(S) is supported");
        }
    };
    let stream = store.get(&path).await?.into_stream();
    Ok(StreamReader::new(stream))
}

#[tokio::main]
async fn main() -> Result<()> {
    if let Some(href) = std::env::args().nth(1) {
        let url = url::Url::parse(&href)?;
        let mut stream_reader = get_async_stream_reader(&url).await?;
        let index = build_bam_index(&mut stream_reader).await?;

        // Write as multi-part stream to other object store...
        // Use local file system for example
        let store = object_store::local::LocalFileSystem::new_with_prefix("./tmp")?;
        let fname = format!("{}.bai", url.path_segments().unwrap().last().unwrap());
        let path: object_store::path::Path = fname
            .clone()
            .try_into()
            .context("Failed to convert filename into path object")?;
        {
            let (_id, mut writer) = store.put_multipart(&path).await?;
            write_bam_index(&mut writer, &index).await?;
            writer.flush().await?;
            writer.shutdown().await?;
        }
        println!("Wrote index to ./tmp/{}", fname);
        Ok(())
    } else {
        anyhow::bail!("No URL provided");
    }
}
