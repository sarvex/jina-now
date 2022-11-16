# Debug instruction
If you want to set break points in the executors, 
you have to set the environment variable `NOW_TESTING==True`. 
This will make the executor run in a single process.

Also make sure to start the playground(port=80) and the bff (port=9090):
```bash
python deployment/bff/app/app.py
streamlit run deployment/playground/playground.py --server.port 80
```