from utils2.common import *
from model import FSRCNN 
import argparse

def load_model(scale):
    ckpt_path = ''
    if scale not in [2, 3, 4]:
        raise ValueError("must be 2, 3 or 4")

    if (ckpt_path == "") or (ckpt_path == "default"):
        ckpt_path = f"checkpoint/x{scale}/FSRCNN-x{scale}.pt"

    sigma = 0.3 if scale == 2 else 0.2

    device = "cuda" if torch.cuda.is_available() else "cpu"

    model = FSRCNN(scale, device)
    model.load_weights(ckpt_path)

    return model



def upscale_image(image_path, scale, model):
    image_path = image_path
    ckpt_path = ''
    scale = scale

    if scale not in [2, 3, 4]:
        raise ValueError("must be 2, 3 or 4")

    if (ckpt_path == "") or (ckpt_path == "default"):
        ckpt_path = f"checkpoint/x{scale}/FSRCNN-x{scale}.pt"

    sigma = 0.3 if scale == 2 else 0.2

    device = "cuda" if torch.cuda.is_available() else "cpu"

    lr_image = read_image(image_path)
    bicubic_image = upscale(lr_image, scale)
    write_image("bicubic.png", bicubic_image)

    lr_image = gaussian_blur(lr_image, sigma=sigma)
    lr_image = rgb2ycbcr(lr_image)
    lr_image = norm01(lr_image)
    lr_image = torch.unsqueeze(lr_image, dim=0)

    
    with torch.no_grad():
        lr_image = lr_image.to(device)
        sr_image = model.predict(lr_image)[0]

    sr_image = denorm01(sr_image)
    sr_image = sr_image.type(torch.uint8)
    sr_image = ycbcr2rgb(sr_image)

    write_image("sr.png", sr_image)

    