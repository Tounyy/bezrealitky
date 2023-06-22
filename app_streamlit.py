import streamlit as st
import pandas as pd
import glob


def load_csv_files(folder_path):
    csv_files = glob.glob(f"{folder_path}/*.csv")
    return csv_files


def load_data(file_path):
    data = pd.read_csv(file_path)
    return data


st.title("Načtení souborů CSV")
folder_path = "data"
csv_files = load_csv_files(folder_path)

if csv_files:
    selected_file = st.selectbox("Vyberte soubor CSV:", csv_files)
    data = load_data(selected_file)
    data.index = data.index + 1

    if st.button("Zobrazit tabulku"):
        st.table(data)
else:
    st.write("V dané složce nebyly nalezeny žádné soubory CSV.")
