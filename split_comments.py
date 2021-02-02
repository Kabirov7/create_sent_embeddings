import re
import emoji


def split_comment_regex_140(text):
	text = emoji.get_emoji_regexp().sub(r'', text.encode().decode('utf8'))
	if len(text) < 140:
		return [text]
	else:
		sentences = re.split(r'(?<=[^А-Я].[. |?|! \n]) +(?=[А-Я \n])', text)
		return sentences


def split_less_then_140(text):
	if len(text) < 140:
		return text
	else:
		return None


def split_by_center_dot(text):
	chars = [".", "!", "?"]
	indices = [0]
	text = emoji.get_emoji_regexp().sub(r"", text.encode().decode("utf8"))
	if len(text) > 140:
		chars_indxs = [i for i, ltr in enumerate(text) if ltr in chars]
		chars_indxs.insert(0, 0)
		if text[-1] not in chars:
			chars_indxs.insert(len(chars_indxs), len(text))

		for i, idx in enumerate(chars_indxs):
			next_i = i
			need_split = False
			while next_i != len(chars_indxs) and not need_split:
				if next_i != i:
					average = (chars_indxs[next_i] - idx) / 2
					if average >= 70:
						split_idx = min(enumerate(chars_indxs), key=lambda x: abs(x[1] - (average)))[1]+1
						if split_idx == indices[-1]:
							average = split_idx+indices[-1]
							split_idx = min(enumerate(chars_indxs), key=lambda x: abs(x[1] - average))[1]+1
						indices.append(split_idx)
						need_split = True

				next_i += 1

		sentences = ["Хехе, я поеду в Узбекистан на сдачу SAT 3 октября."[i:j].strip() for i, j in zip(indices, indices[1:] + [None])]
		sentences = [x for x in sentences if x not in [""]]
		return sentences
	else:
		return [text]


text = "Хехе, я поеду в Узбекистан на сдачу SAT 3 октября! Мало того что проворонила все лето за учебой, так теперь теперь еду в другую страну сдавать! т сраный тест который не факт что хорошо напишу? "
text1 = "Хехе, я поеду в Узбекистан на сдачу SAT 3 октября."

print(split_by_center_dot(text))
# print(split_by_center_dot(text1))
