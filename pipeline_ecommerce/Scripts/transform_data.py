from extract_and_save import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd

def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

def select_category(col, category):
    query = { "Categoria do Produto": f"{category}"}
    
    lista_categoria = []
    for doc in col.find(query):
        lista_categoria.append(doc)

    return lista_categoria

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

def create_dataframe(lista):
    df =  pd.DataFrame(lista)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")


if __name__ == "__main__":
    
    client = connect_mongo("mongodb+srv://arthurtelescoutinho:!Dezembro15@cluster-pipeline.w4oto0g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-pipeline")
    db = create_connect_db(client, "db_produtos")
    col = create_connect_collection(db, "produtos")
    
    rename_column(col,'lat','Latitude')
    rename_column(col,'lon','Longitude')
    
    livros_list = select_category(col,'livros')
    df_livros = create_dataframe(livros_list)
    format_date(df_livros)
    save_csv(df_livros,'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\pipeline_ecommerce\\processed_data\\livros.csv')
    
    produtos_list = make_regex(col,'/202[1-9]')
    df_produtos = create_dataframe(livros_list)
    format_date(df_produtos)
    save_csv(df_produtos,'C:\\Users\\Arthur Coutinho\\Desktop\\Arthur Coutinho\\Python\\Data_Engineering\\pipeline_ecommerce\\processed_data\\livros.csv')
