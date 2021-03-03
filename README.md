# Hotel Data Processing

This project is a collection of tools to process hotel data. Here is a list of tools.(working in progress)
1. analytic: TBD
2. image: collecting images and room classification experiments


## Getting Started

Please setup environment with python and jupyter lab


### Prerequisites

add .env file under module folder which needs to have all credentials for DB access
TBD

## image module
#### Prepare list of the image names for download
- run image_main.ipynb
#### download the images as listed by images.csv
- update run_image_download.py with corresponding file structure and run command
  
  ```python run_image_download.py -i ./hotel_images/valid/images.csv -b . -o hotel_images -g valid -s 50```
  
  for help: ```python run_image_download.py -h ```

#### training images based on the images
- run model.ipynb



Note: still in process identify missing supporting files and latest updates may be required to get the code to be fully functional.
Also need to audit the files for any hard-coded credentials before adding them.

Read csv with pandas: 
1589 iterations with batch size of 25 5207 sec 
795 iterations with batch size of 50 5348 sec
