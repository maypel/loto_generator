from cols_datasets import col_to_delete, col_order

import os
import pandas as pd

def prepare_and_concatenate_csv_files(csv_dir):
    """
    Parcourt un répertoire, lit tous les fichiers CSV avec le bon délimiteur, harmonise leurs colonnes et les concatène.
    
    :param csv_dir: Répertoire où se trouvent les fichiers CSV
    :return: Un DataFrame Pandas contenant toutes les données concaténées
    """
    csv_files = []  # Liste pour stocker les DataFrames de chaque fichier CSV
    all_columns = set()  # Ensemble de toutes les colonnes rencontrées
    
    # Première étape : Collecter toutes les colonnes
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_dir, filename)
            print(f"Chargement du fichier {filename} pour collecte des colonnes")
            # Spécifier l'encodage et le bon séparateur
            df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=';')  
            all_columns.update(df.columns)  # Mise à jour des colonnes
    
    # Deuxième étape : Harmoniser et stocker les fichiers CSV avec les colonnes complètes
    all_columns = list(all_columns)  # Convertir en liste pour préserver l'ordre
    for filename in os.listdir(csv_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(csv_dir, filename)
            print(f"Harmonisation et chargement de {filename}")
            # Lire à nouveau avec le bon séparateur
            df = pd.read_csv(file_path, encoding='ISO-8859-1', sep=';')
            
            # Ajouter les colonnes manquantes avec des NaN
            for col in all_columns:
                if col not in df.columns:
                    df[col] = pd.NA
            
            # Réorganiser les colonnes dans le bon ordre
            df = df[all_columns]
            
            # Ajouter le DataFrame à la liste
            csv_files.append(df)
    
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


def handle_cols(columns_to_remove, column_order, csv_file):
    # Lire le fichier CSV
    combined_csv = pd.read_csv(csv_file, sep=';')

    # Étape 1 : Afficher les colonnes pour vérifier les noms actuels
    print("Colonnes disponibles :", combined_csv.columns.tolist())

    # Étape 2 : Nettoyer les noms des colonnes pour supprimer tout caractère indésirable
    combined_csv.columns = combined_csv.columns.str.strip().str.replace(r'[^a-zA-Z0-9_]', '', regex=True)

    # Étape 3 : Supprimer les colonnes non désirées, tout en vérifiant si elles existent
    combined_csv = combined_csv.drop(columns=[col for col in columns_to_remove if col in combined_csv.columns], errors='ignore')

    # Étape 4 : Vérifier si les colonnes de column_order sont présentes dans le DataFrame
    missing_cols = [col for col in column_order if col not in combined_csv.columns]
    if missing_cols:
        print(f"Attention : Les colonnes suivantes ne sont pas présentes dans le DataFrame : {missing_cols}")

    # Étape 5 : Réorganiser les colonnes dans l'ordre désiré, en s'assurant que seules les colonnes existantes sont utilisées
    combined_csv = combined_csv[[col for col in column_order if col in combined_csv.columns]]

    # Étape 6 : Sauvegarder le résultat dans un nouveau fichier CSV
    output_file = 'cleaned_combined_dataset.csv'
    combined_csv.to_csv(output_file, index=False, sep=';')
    print(f"Le fichier CSV nettoyé et réorganisé a été sauvegardé sous le nom : {output_file}")


# Exemple d'utilisation
if __name__ == "__main__":
    csv_directory = "cleaned_datasets"  # Répertoire où se trouvent les fichiers CSV
    
    # Concaténer tous les fichiers CSV dans un DataFrame après harmonisation
    df = prepare_and_concatenate_csv_files(csv_directory)

    
    if df is not None:
        print("Concaténation terminée. Aperçu du DataFrame harmonisé :")
        print(df.head())  # Afficher les 5 premières lignes du DataFrame
        
        # Sauvegarder le DataFrame concaténé dans un fichier CSV avec le bon délimiteur
        save_combined_dataset(df, 'combined_dataset.csv')    
        handle_cols(col_to_delete,col_order,'combined_dataset.csv')  
        