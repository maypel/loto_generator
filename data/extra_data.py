import os
import requests
import shutil
import zipfile
from bs4 import BeautifulSoup


# Fonction pour télécharger un fichier à partir d'un URL
def download_file(url, output_dir):
    local_filename = os.path.join(output_dir, url.split("/")[-1])  # Nom du fichier à partir de l'URL
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"Téléchargé : {local_filename}")
    return local_filename


# Fonction 1: Dézipper les fichiers
def unzip_files(zip_dir, extract_dir):
    """
    Parcourt un répertoire contenant des fichiers ZIP (même sans extension .zip),
    et les extrait dans un répertoire spécifié.
    
    :param zip_dir: Répertoire où se trouvent les fichiers ZIP
    :param extract_dir: Répertoire où les fichiers seront extraits
    """
    # Créer le répertoire d'extraction s'il n'existe pas
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    
    # Parcourir les fichiers du répertoire zip_dir
    for filename in os.listdir(zip_dir):
        file_path = os.path.join(zip_dir, filename)
        
        # Essayer d'ouvrir chaque fichier comme un fichier ZIP
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                print(f"Décompression de {filename}")
                zip_ref.extractall(extract_dir)
        except zipfile.BadZipFile:
            print(f"Le fichier {filename} n'est pas un fichier ZIP valide, il est ignoré.")

# Fonction 2: Déplacer le fichier extrait
def move_extracted_files(extract_dir, dest_dir):
    """
    Déplace les fichiers extraits vers un répertoire de destination.
    
    :param extract_dir: Répertoire contenant les fichiers extraits
    :param dest_dir: Répertoire où les fichiers doivent être déplacés
    """
    # Créer le répertoire de destination s'il n'existe pas
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    
    # Parcourir les fichiers extraits et les déplacer
    for root, dirs, files in os.walk(extract_dir):
        for file in files:
            file_path = os.path.join(root, file)
            # Déplacer chaque fichier extrait vers le répertoire de destination
            print(f"Déplacement de {file} vers {dest_dir}")
            shutil.move(file_path, os.path.join(dest_dir, file))

# Fonction 3: Supprimer les fichiers et dossiers inutiles
def cleanup(zip_dir, extract_dir):
    """
    Supprime les fichiers ZIP et les dossiers temporaires après le traitement.
    
    :param zip_dir: Répertoire où se trouvent les fichiers ZIP
    :param extract_dir: Répertoire temporaire d'extraction
    """
    # Supprimer tous les fichiers dans zip_dir
    for filename in os.listdir(zip_dir):
        file_path = os.path.join(zip_dir, filename)
        print(f"Suppression de {file_path}")
        os.remove(file_path)
    
    # Supprimer le répertoire temporaire d'extraction
    if os.path.exists(extract_dir):
        print(f"Suppression du dossier temporaire {extract_dir}")
        shutil.rmtree(extract_dir)
    if os.path.exists(zip_dir):
        print(f"Suppression du dossier datasets_euromillions {zip_dir}")
        shutil.rmtree(zip_dir)

def Main():
    # URL de la page contenant les datasets
    url = "https://www.fdj.fr/jeux-de-tirage/euromillions-my-million/historique"

    # Dossier où les fichiers seront enregistrés
    output_dir = "datasets_euromillions"

    # Créer le dossier s'il n'existe pas
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


    # Envoyer une requête pour obtenir le contenu de la page
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Erreur lors de la requête HTTP : {response.status_code}")
        exit()

    # Analyser le contenu HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extraire toutes les balises <a> avec la classe "block"
    links = soup.find_all('a', class_='block')

    # Filtrer les liens valides et télécharger les fichiers
    for link in links:
        file_url = link.get('href')  # Extraire le lien de téléchargement
        download_file(file_url, output_dir)

    print("Tous les fichiers ont été téléchargés.")

    zip_directory = "datasets_euromillions"  # Dossier contenant les fichiers ZIP téléchargés
    temp_extract_directory = "temp_extract"   # Dossier temporaire pour l'extraction
    destination_directory = "raw_datasets"  # Dossier final pour stocker les fichiers extraits

    # Dézipper les fichiers
    unzip_files(zip_directory, temp_extract_directory)

    # Déplacer les fichiers extraits
    move_extracted_files(temp_extract_directory, destination_directory)

    # Nettoyer les fichiers ZIP et les dossiers inutiles
    cleanup(zip_directory, temp_extract_directory)

# Exemple d'utilisation des fonctions
if __name__ == "__main__":
    Main()
