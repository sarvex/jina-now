from collections import defaultdict

from docarray import DocumentArray


def merge_matches_sum(query_docs, limit):
    # in contrast to merge_matches_min, merge_matches_avg sorts the parent matches by the average distance of all chunk matches
    # we have 3 chunks indexed for each root document but the matches might contain less than 3 chunks
    # in case of less than 3 chunks, we assume that the distance of the missing chunks is the same to the last match
    # m.score.value is a distance metric
    query_doc = query_docs[0]
    parent_id_count_and_sum_and_chunks = defaultdict(lambda: [0, 0, []])
    for m in query_doc.matches:
        count_and_sum_and_chunks = parent_id_count_and_sum_and_chunks[m.parent_id]
        distance = m.scores['cosine'].value
        count_and_sum_and_chunks[0] += 1
        count_and_sum_and_chunks[1] += distance
        count_and_sum_and_chunks[2].append(m)
    all_matches = DocumentArray()
    for group in (3, 2, 1):
        parent_id_to_sum_and_chunks = {
            parent_id: count_and_sum_and_chunks[1:]
            for parent_id, count_and_sum_and_chunks in parent_id_count_and_sum_and_chunks.items()
            if count_and_sum_and_chunks[0] == group
        }
        parent_to_sum_sorted = sorted(
            parent_id_to_sum_and_chunks.items(), key=lambda x: x[1][0]
        )
        matches = [sum_and_chunks[1][0] for _, sum_and_chunks in parent_to_sum_sorted]
        all_matches.extend(matches)
        print(f'# num parents for group {group}: {len(matches)}')
    query_doc.matches = all_matches[:limit]
