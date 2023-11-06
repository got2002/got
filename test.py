from PIL import Image
import sys
import os
import psycopg2
import pandas as pd
from pathlib import Path
import logging  
from datetime import datetime

#input_path = "C:\\Users\\gohvf\\Downloads\\Cartiage Case\\Bullet\\"

log_format = "%(message)s"  # Only include the message
start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
start_log_message = f"{start_time} [INFO] start"
end_log_message = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [INFO] end\n"

# Configure logging to both console and file
logging.basicConfig(filename='error.log', level=logging.INFO, format=log_format, encoding='utf-8')
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(console_handler)

logging.info(start_log_message) 
try:
    
    line_cases_start = 2
    line_cases_end = 6
    line_bullet_start = 8
    line_bullet_end = 12
    line_db = 14
    

except Exception as e:
    error_message = f"Error : configuration error occurred line.{str(e)}"
    print(error_message)
    logging.error(error_message)
    

def crop_and_save_images_from_db(input_cases,input_bullet,output_dir):
    try:

        config_file_path = Path("./config.txt")
        with config_file_path.open('r', encoding='utf-8') as crop_file:
            lines = crop_file.readlines()

            scale_crop_cases = [tuple(map(float, line.strip().split(','))) for line in lines[line_cases_start:line_cases_end]]
            
            scale_crop_bullet = [tuple(map(float, line.strip().split(','))) for line in lines[line_bullet_start:line_bullet_end]]
            DB_param = tuple(lines[line_db].split(','))
        #print(DB_param)
        
        conn = psycopg2.connect(database=DB_param[0].strip(),
                                host=DB_param[1].strip(),
                                user=DB_param[2].strip(),
                                password=DB_param[3].strip(),
                                port=DB_param[4].strip())
        
        cursor = conn.cursor()
        
        
    except Exception as e:
        error_message = f"Error : connecting to the database / Error : {str(e)}"
        print(error_message)
        logging.error(error_message)
        


        cmd_ex = "Select * from \"View_jpgCases\""
        try:
            cursor.execute(cmd_ex)
            rows = cursor.fetchall()
            

        except Exception as sql_error:
            # บันทึก error ของคำสั่ง SQL ที่ไม่สามารถประมวลผลได้
            error_message = f"Error : executing SQL command for  Cases - {str(sql_error)}"
            print(error_message)
            logging.error(error_message)
            return


        for i, row in enumerate(rows):
            try:
                image_path = os.path.join(input_cases, f"{row[1]}.jpeg")
                img = Image.open(image_path)
                width, height = img.size

                os.makedirs(output_dir, exist_ok=True)

                for j, crop_coords in enumerate(scale_crop_cases):
                    crop_coords = tuple(int(val * width if idx % 2 == 0 else val * height) for idx, val in enumerate(crop_coords))
                    cropped_img = img.crop(crop_coords)
                    output_image_path = os.path.join(output_dir, f"{row[0]}_{j + 1}.jpg")

                    if not cropped_img.getbbox():
                        print(f"Skipping empty crop: {output_image_path}")
                    else:
                        cropped_img.save(output_image_path)
                        # print(f"Cropped image {i + 1}_{j + 1} saved to {output_image_path}")
                    
            except Exception as e:
                # บันทึก error ที่เกิดขึ้นในส่วนนี้
                error_message = f" Error : while processing image {row[0]} in Cases - {str(e)} "
                print(error_message)
                logging.error(error_message)

        # cases finish

        # ส่วนการดึงข้อมูลจากฐานข้อมูลสำหรับ folder 2 Bullet
        cmd_ex = "Select * from \"View_jpgBullet\""
        try:
            cursor.execute(cmd_ex)
            rows = cursor.fetchall()
            
        except Exception as sql_error:
            # บันทึก error ของคำสั่ง SQL ที่ไม่สามารถประมวลผลได้
            error_message = f" Error : executing SQL command for Bullet - {str(sql_error)}"
            print(error_message)
            logging.error(error_message)
            return

        for i, row in enumerate(rows):
            try:
                image_path = os.path.join(input_bullet, f"{row[1]}.jpeg")
                img = Image.open(image_path)
                width, height = img.size

                os.makedirs(output_dir, exist_ok=True)

                for j, crop_coords in enumerate(scale_crop_bullet):
                    crop_coords = tuple(int(val * width if idx % 2 == 0 else val * height) for idx, val in enumerate(crop_coords))
                    cropped_img = img.crop(crop_coords).rotate(90, expand=True)
                    output_image_path = os.path.join(output_dir, f"{row[0]}_{j + 1}.jpg")

                    if not cropped_img.getbbox():
                        print(f"Skipping empty crop: {output_image_path}")
                    else:
                        cropped_img.save(output_image_path)
                        # print(f"Cropped image {i + 1}_{j + 1} saved to {output_image_path}")
                
            except Exception as e:
                # บันทึก error ที่เกิดขึ้นในส่วนนี้
                error_message = f"Error while processing image {row[0]} in  Bullet - {str(e)}"  
                logging.error(error_message)

        # bullet finish
        conn.close()
          
    except Exception as e:
                # บันทึก error ที่เกิดขึ้นในส่วนนี้
        error_message = f"Error : {str(e)}"  
        logging.error(error_message)
 

    
    
if __name__ == "__main__":
    
    if len(sys.argv) != 4:
        error_message = "Error: Invalid number of arguments. input_directory_cases input_directory_bullet output_directory"
        print(error_message)
        logging.error(error_message)
        logging.info(end_log_message)
        sys.exit(1)
    
    input_cases = sys.argv[1]   
    input_bullet = sys.argv[2]
    output_dir = sys.argv[3]

    # ตรวจสอบว่าได้รับพาธที่ถูกต้อง
    if not os.path.isdir(input_cases) or not os.path.isdir(input_bullet):
        error_message ="Error: Invalid input directory path"
        print(error_message)
        
        logging.error(error_message)
        logging.info(end_log_message)
        sys.exit(1)
    
    # ตรวจสอบว่าได้รับพาธเป้าหมายที่ถูกต้อง
    if not os.path.exists(output_dir):
        error_message = "Error: Output directory does not exist"
        
        print(error_message)
        logging.error(error_message)
        logging.info(end_log_message)   
        sys.exit(1)
    
    # Log that conditions are correct
    logging.log(logging.INFO, "[OK] Script starting with the correct number of arguments and valid directories.")
    
    crop_and_save_images_from_db(input_cases, input_bullet, output_dir)
logging.info(end_log_message)
    