import streamlit.web.cli as stcli
import sys
sys.argv = ["streamlit", "run", "app_streamlit.py"]
stcli.main()
