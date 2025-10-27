# smartmic-tiff2dicom
This repo converts a TIFF image to the DICOM format. 
We are using the converter covered in this([NIH paper](https://pmc.ncbi.nlm.nih.gov/articles/PMC8274303/)).

https://github.com/Steven-N-Hart/dicom_wsi

Usage: 

```bash
python -m dicom_wsi.cli   
  -w pathway_to_your.tiffile.tif 
  -o output_directory 
  -p name_of_your_output 
  -y base.yaml

```

You will have to change the first line in your yaml file according to which TIF file you are using.

```
WSIFile: 'pathway_to_your.tiffile.tif'

```