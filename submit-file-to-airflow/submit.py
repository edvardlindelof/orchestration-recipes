import pandas as pd
from util import submit_film_file


df = pd.DataFrame({
    "name": ["Inception", "The Matrix"],
    "lead_actor": ["Leonardo DiCaprio", "Keanu Reeves"],
    "rating": [8, 9]
})

submit_film_file(df, "films.csv")
print("DAG triggered successfully")
