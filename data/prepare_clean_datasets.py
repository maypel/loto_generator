from cols_datasets import col_order

import os
import pandas as pd
from typing import Optional


def normalize_date_format(df, date_column)->pd.DataFrame:
    # Convertir la colonne en format datetime, avec gestion des formats variés
    df[date_column] = df[date_column].astype(str)
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce', 
                                      exact=False, 
                                     infer_datetime_format=True)

    # Reformater la date en 'jj/mm/aaaa'
    df[date_column] = df[date_column].dt.strftime('%d/%m/%Y')
    
    return df


def remove_cols(column_order,csv_dir)-> Optional[pd.DataFrame]:
    """
     Parcourt un répertoire, lit tous les fichiers CSV avec le bon délimiteur et harmonise leurs colonnes.
    
    :param csv_dir: Répertoire où se trouvent les fichiers CSV
    :return: Un DataFrame Pandas contenant toutes les données concaténées
    
    """
    # Création du répertoire de destination
    dest_dir = "cleaned_datasets"
    csv_files = []  # Liste pour stocker les DataFrames de chaque fichier CSV
    if not os.path.exists(dest_dir): # s'il n'existe pas, je le crée
        os.makedirs(dest_dir)

    for filename in os.listdir(csv_dir): # je liste les documents dans mon répertoire source
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_dir, filename)
            print(f"Chargement du fichier {filename} pour collecte des colonnes")
            # Spécifier l'encodage et le bon séparateur
            df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=';', index_col=False)  
           
           # Réorganiser les colonnes dans l'ordre désiré tout en supprimant les colonnes inutiles
            df = df[[col for col in column_order if col in df.columns]]
            df = normalize_date_format(df,'date_de_tirage')

            csv_files.append(df)

            # Sauvegarder le résultat dans un nouveau fichier CSV
            output_file = f'{dest_dir}\\cleaned_{filename}'
            df.to_csv(output_file, index=False, sep=';')
            print(f"Le fichier CSV nettoyé et réorganisé a été sauvegardé sous le nom : {output_file}")
    
    # Concaténer tous les DataFrames dans un seul
    if csv_files:
        concatenated_df = pd.concat(csv_files, ignore_index=True)
        return concatenated_df
    else:
        print("Aucun fichier CSV trouvé.")
        return None
 

def save_combined_dataset(df, output_file):
    """
    Sauvegarde le DataFrame concaténé dans un fichier CSV avec le bon délimiteur.
    
    :param df: DataFrame à sauvegarder
    :param output_file: Chemin du fichier CSV de sortie
    """
    try:
        with open(output_file, 'w', encoding='utf-8', newline='') as file:
            df.to_csv(file, index=False, sep=';')  # Utiliser ';' comme séparateur
        print(f"Le fichier {output_file} a été sauvegardé avec succès.")
    except PermissionError:
        print(f"Erreur : Permission refusée pour le fichier {output_file}. Assurez-vous qu'il n'est pas ouvert ailleurs.")
    except Exception as e:
        print(f"Erreur lors de la sauvegarde : {str(e)}")



# Exemple d'utilisation
if __name__ == "__main__":
    csv_directory = "raw_datasets"  # Répertoire où se trouvent les fichiers CSV
    df = remove_cols(column_order=col_order,csv_dir=csv_directory)
    
    if df is not None:
        print("Concaténation terminée. Aperçu du DataFrame harmonisé :")
        print(df.head())  # Afficher les 5 premières lignes du DataFrame
        
        # Sauvegarder le DataFrame concaténé dans un fichier CSV avec le bon délimiteur
        save_combined_dataset(df, 'combined_cleaned_dataset.csv')    
        