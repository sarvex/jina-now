import copy
from collections import defaultdict

from finetuner.tuner.evaluation import Evaluator
from jina import DocumentArray
from src.hub.head_encoder.head_encoder import FineTunedLinearHeadEncoder
from src.utils import visual_result, save_before_after_image, plot_metrics
import numpy as np

np.seterr(divide='ignore', invalid='ignore')

random_state = 42


def show_improvement(
    data,
    query,
    index,
    final_layer_output_dim,
    embedding_size,
    finetuned_model_path,
    top_x=10,
    class_label='finetuner_label',
):
    """
    The user should see some visualizations on how the pre-trained model performed compared to the fine-tuned to show
    the power of fine-tuner.
    1. show search results before/ after in a flashy way
    2. show the linear probing ability of the model before/after (kalims graphics)

    Ideas:
    a) you could start an HTML page
    b) you could plt.show() some graphics and save them on disc
    """
    # 1: Show the linear probing ability of the model before/after
    # Step 1: Prepare filtered dataset
    doc_to_classes = defaultdict(int)
    for d in index:
        doc_to_classes[d.tags[class_label]] += 1

    # sort the filtered doc by their count in decreasing order
    tuples = [(category, count) for category, count in doc_to_classes.items()]
    tuples.sort(key=lambda x: x[1], reverse=True)
    # pick top_x classes from the docs
    top_x_classes = [x[0] for idx, x in enumerate(tuples) if idx < top_x]

    # now filter the doc based on the top_x_classes and take max 2K documents from each classes
    labels = []
    subset = []
    label_dict = {}
    label_id = 0
    for d in index:
        cat = d.tags[class_label]
        if cat not in top_x_classes:
            continue
        if cat not in label_dict:
            label_dict[cat] = label_id
            label_id += 1
        if not doc_to_classes[cat] > 2000:
            subset.append(copy.deepcopy(d))
            labels.append(label_dict[cat])
    filtered_doc = DocumentArray(subset)

    # Step 2: Get pretrained and finetuned embeddings
    pretrained_embed = filtered_doc.embeddings
    finetuned_encoder = FineTunedLinearHeadEncoder(
        final_layer_output_dim, embedding_size, model_path=finetuned_model_path
    )
    finetuned_docs = finetuned_encoder.encode(filtered_doc)
    finetuned_embed = finetuned_docs.embeddings

    # # Step 3: Calling SVM on finetuned & pretrained embed
    # get_pr_curve(pretrained_embed, labels, title='pretrained_pr')
    # get_pr_curve(finetuned_embed, labels, title='finetuned_pr')

    # 2. Get search results/metric and plot together
    # Step 1: Pre-trained
    query.match(index, limit=9, exclude_self=True)
    visual_result(data, query, output='pretrained.png', label=class_label)
    evaluator = Evaluator(query_data=query, index_data=index)
    ev = evaluator.evaluate()
    plot_metrics(ev, 'pretrained_m.png')

    # Fine-tuned
    new_query = finetuned_encoder.encode(query)
    new_index = finetuned_encoder.encode(index)
    new_query.match(new_index, limit=9, exclude_self=True)
    visual_result(data, new_query, output='finetuned.png', label=class_label)
    evaluator = Evaluator(query_data=query, index_data=index)
    ev = evaluator.evaluate()
    plot_metrics(ev, 'finetuned_m.png')

    # saving all the before-after images side-by-side
    save_before_after_image(path=f'/root/data/before_after_comparison_{data}.png')