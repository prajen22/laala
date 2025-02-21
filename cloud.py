import streamlit as st
import fitz  # PyMuPDF for PDF processing
import tempfile
from imagekitio import ImageKit
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os

# ✅ Connect to Elastic Cloud with API Key
ELASTICSEARCH_URL = "https://e4d509b4d8fb49a78a19a571c1b65bba.us-central1.gcp.cloud.es.io:443"
API_KEY = "dHlnVEtaVUJCYWNWcEcwczVQcE46d2tOTURWLXBUSmFvQkg1bmxma1VkQQ=="  # Replace with your actual API key

es = Elasticsearch(
    ELASTICSEARCH_URL,
    api_key=API_KEY
)

# ✅ Initialize ImageKit SDK
imagekit = ImageKit(
    private_key='private_lJZeBuXRen5WI4WpjNRjf1DZW4E=',
    public_key='public_djwqIa18ksHGZEGTJk59MFOp/mA=',
    url_endpoint='https://ik.imagekit.io/46k1lkvq2'
)

# ✅ Elasticsearch Index Name
index_name = "pdf_documents"

# ✅ Create an index if it doesn't exist
if not es.indices.exists(index=index_name):
    es.indices.create(
        index=index_name,
        body={
            "mappings": {
                "properties": {
                    "pdf_name": {"type": "text"},
                    "page_number": {"type": "integer"},
                    "page_content": {"type": "text"},
                    "imagekit_link": {"type": "keyword"}
                }
            }
        }
    )

def upload_to_imagekit(file_path, file_name):
    """Uploads a file to ImageKit.io and returns the file URL."""
    try:
        with open(file_path, "rb") as file:
            response = imagekit.upload(file=file, file_name=file_name)
        return response.url if hasattr(response, "url") else None
    except Exception as e:
        return None

def process_and_store(pdf_path):
    """Uploads the PDF to ImageKit, extracts text, and stores data in Elasticsearch."""
    pdf_document = fitz.open(pdf_path)
    pdf_name = os.path.basename(pdf_path)
    
    # ✅ Upload full PDF to ImageKit
    pdf_cdn_link = upload_to_imagekit(pdf_path, pdf_name)
    if not pdf_cdn_link:
        return None

    # ✅ Extract text & store each page separately in Elasticsearch
    actions = []
    for page_num in range(len(pdf_document)):
        page = pdf_document[page_num]
        page_text = page.get_text("text").strip()
        page_link = f"{pdf_cdn_link}#page={page_num + 1}"

        # ✅ Prepare bulk insert action
        actions.append({
            "_index": index_name,
            "_source": {
                "pdf_name": pdf_name,
                "page_number": page_num + 1,
                "page_content": page_text,
                "imagekit_link": page_link
            }
        })

    bulk(es, actions)  # ✅ Bulk insert all pages at once
    return pdf_cdn_link

def search_pdfs(query):
    """Search PDFs stored in Elasticsearch based on a query."""
    search_body = {"query": {"match": {"page_content": query}}}
    response = es.search(index=index_name, body=search_body)
    return response["hits"]["hits"]

# 🔹 Streamlit UI
st.title("📂 PDF Upload & Search (Elastic Cloud)")

# ✅ Upload PDF Section
uploaded_file = st.file_uploader("Upload a PDF", type=["pdf"])

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as temp_pdf:
        temp_pdf.write(uploaded_file.getbuffer())
        temp_pdf_path = temp_pdf.name

    st.info("Processing PDF and uploading...")
    pdf_url = process_and_store(temp_pdf_path)

    if pdf_url:
        st.success("✅ PDF successfully uploaded and stored in Elasticsearch!")
        st.write(f"🔗 **Full PDF Link:** [View PDF]({pdf_url})")
    else:
        st.error("❌ Upload failed. Please try again.")

# ✅ Search Section
query = st.text_input("🔍 Search for text in PDFs:")
if query:
    results = search_pdfs(query)
    if results:
        for res in results:
            st.write(f"📌 **PDF:** {res['_source']['pdf_name']}")
            st.write(f"📄 **Page {res['_source']['page_number']}**")
            st.write(f"🔗 **[View Page]({res['_source']['imagekit_link']})**")
            st.write(f"📝 **Excerpt:** {res['_source']['page_content'][:200]}...\n")
    else:
        st.warning("No results found.")

