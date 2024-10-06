'''
使用videollama2进行视频模态转文字模态，模型下载下来太大传不上去，运行时在huggingface在线下载（可以去huggingface手动下载）
输入输出文件路径待修改
'''
import sys
sys.path.append('./')
from VideoLLaMA2.videollama2 import model_init, mm_infer
from VideoLLaMA2.videollama2.utils import disable_torch_init

input_path = ''


def inference():
    disable_torch_init()

    modal = 'video'
    modal_path = input_path
    instruct = "Please convert the video into text data that can be further processed based on its content and meaning."

    model_path = 'DAMO-NLP-SG/VideoLLaMA2-7B'
    model, processor, tokenizer = model_init(model_path)

    output = mm_infer(processor[modal](modal_path), instruct, model=model, tokenizer=tokenizer, do_sample=False, modal=modal)
    print(output)
    #未指定输出

if __name__ == "__main__":
    inference()
