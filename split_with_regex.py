import re
import emoji
import psycopg2
import psycopg2.extras

db_params = {
	'host': 'localhost',
	'port': 5432,
	'database': 'comments',
	'user': 'postgres',
	'password': 'postgres'
}

db = psycopg2.connect(**db_params)
cursor = db.cursor()


def split_comment(text):
	text = emoji.get_emoji_regexp().sub(r'', text.encode().decode('utf8'))
	if len(text) < 140:
		return [text]
	else:
		sentences = re.split(r'(?<=[^А-Я].[. |?|! \n]) +(?=[А-Я \n])', text)
		return sentences


cursor.execute("select comment_text from insta_news_comments")
all = cursor.fetchall()

rows = []
for i in all:
	sentences = split_comment(i[0])
	for sent in sentences:
		row = {
			"sent": sent,
			"full_sent": i[0],
			"count_words_sent": len(sent.split()),
			"count_letters_sent": len(sent),
		}
		rows.append(row)

print(rows)
psycopg2.extras.execute_batch(cursor, """insert into comments_by_sent(sent, full_sent, count_words_sent, count_letters_sent)
	values (
		%(sent)s,
		%(full_sent)s,
		%(count_words_sent)s,
		%(count_letters_sent)s
	)
	ON CONFLICT DO NOTHING""", rows)
db.commit()
