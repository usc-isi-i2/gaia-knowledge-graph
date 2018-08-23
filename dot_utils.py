type_color_map = {
    # Entity
    'Facility': '#7f7f7f',
    'GeopoliticalEntity': '#e377c2',
    'Location': '#8c564b',
    'Organization': '#9467bd',
    'Person': '#1f77b4',
    'FillerType': '#ff7f0e'
    # Event
}


def node_color(type_):
    color = type_color_map.get(type_, '#17becf')
    return ', fillcolor="{}", color="{}"'.format(color, color)


def node_label_justify(label, count, max_width=20):
    words = label + " (×{})".format(count)
    return "\\n".join(text_justify(words, max_width))


def text_justify(words, max_width):
    words = words.split()
    res, cur, num_of_letters = [], [], 0
    max_ = 0
    for w in words:
        if num_of_letters + len(w) + len(cur) > max_width:
            res.append(' '.join(cur))
            max_ = max(max_, num_of_letters)
            cur, num_of_letters = [], 0
        cur.append(w)
        num_of_letters += len(w)
    return res + [' '.join(cur).center(max_)]


def generate(node_strings, edge_strings):
    dot = [
        'digraph G {',
        '  node[style="filled"]',
    ]
    dot.extend(node_strings)
    dot.extend(edge_strings)
    dot.append('}')
    return '\n'.join(dot)
