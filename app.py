# app.py
import streamlit as st
from validator import run_validation

st.set_page_config(page_title="Data Validator", layout="centered")
st.title("📊 Dataset Comparison Validator")

# File upload
uploaded_file1 = st.file_uploader("Upload Current Dataset (CSV or TXT)", type=["csv", "txt"], key="file1")
uploaded_file2 = st.file_uploader("Upload Previous Dataset (CSV or TXT)", type=["csv", "txt"], key="file2")

# Country selection
country = st.selectbox("Select Country", ["USA", "India", "Germany", "UK", "CA"])

# Trigger validation
if uploaded_file1 and uploaded_file2 and st.button("Run Validation"):
    # Save the files temporarily
    file1_path = f"temp_current.{uploaded_file1.name.split('.')[-1]}"
    file2_path = f"temp_previous.{uploaded_file2.name.split('.')[-1]}"

    with open(file1_path, "wb") as f1:
        f1.write(uploaded_file1.read())
    with open(file2_path, "wb") as f2:
        f2.write(uploaded_file2.read())

    st.info("Running validation... Please wait.")

    # Pass file paths and let validator handle the delimiter
    log_file = run_validation(file1_path, file2_path, country)

    st.success("Validation completed. Showing log below:")
    with open(log_file, "r") as lf:
        log_content = lf.read()
        st.text_area("Validation Log", log_content, height=400)

    st.download_button(
        label="📥 Download Log File",
        data=log_content,
        file_name=log_file.split("/")[-1],
        mime="text/plain"
    )
