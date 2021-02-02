import psycopg2
import psycopg2.extras
import db_params
import re
import tensorflow_hub as hub
import json
import numpy as np
import emoji
import math
import tensorflow_text
from pprint import pprint
from datetime import datetime


class LukoshkoTextToEmbeddings:
	def __init__(self, db_params, table_name):
		self._create_table(db_params, table_name)
		self.embed = hub.load("https://tfhub.dev/google/universal-sentence-encoder-multilingual/3")

	def _create_table(self, db_params, table_name):
		conn = None
		try:
			conn = psycopg2.connect(**db_params)
			with conn.cursor() as cursor:
				cursor.execute(
					f'create table if not exists {table_name}('
					f'post_id bigint,'
					f'comment_id bigint,'
					f'comment_text text,'
					f'sentence_idx int,'
					f'sentence text,'
					f'clean_sentence text,'
					f'owner_id bigint,'
					f'owner_username text,'
					f'comment_date text,'
					f'serialized_array_meta text,'
					f'array_bytes bytea,'
					f'idx bigint not null,'
					f'faiss_index serial not null,'
					f'UNIQUE (owner_id, comment_id, sentence, clean_sentence))')
				conn.commit()
		except Exception as e:
			print(e)
		finally:
			if conn:
				conn.close()

	def _insert_sentence(self, conn, rows, table_name):
		try:
			with conn.cursor() as cursor:
				print("try")
				psycopg2.extras.execute_batch(cursor,
											  f"""
                                    INSERT INTO {table_name}(
                                         post_id,
										 comment_id,
										 comment_text,
										 sentence_idx,
										 sentence,
										 clean_sentence,
										 owner_id,
										 owner_username,
										 comment_date,
										 serialized_array_meta,
										 array_bytes,
										 idx
                                     )
									VALUES (
										%(post_id)s,
										%(comment_id)s,
										%(comment_text)s,
										%(sentence_idx)s,
										%(sentence)s,
										%(clean_sentence)s,
										%(owner_id)s,
										%(owner_username)s,
										%(comment_date)s,
										%(serialized_array_meta)s,
										%(array_bytes)s,
										%(idx)s

									)
									ON CONFLICT ON CONSTRAINT insta_news_comments_with_embe_owner_id_comment_id_sentence__key DO NOTHING;""",
											  rows, page_size=500)
			conn.commit()
		except Exception as e:
			print(e)

	def create_embeddings(self, db_params, source_table_name, target_table_name):
		conn = None
		insert_conn = None

		try:
			conn = psycopg2.connect(**db_params)
			insert_conn = psycopg2.connect(**db_params)
			rows = []
			with conn.cursor('server_side_cursor', cursor_factory=psycopg2.extras.DictCursor) as cursor:
				cursor.execute(f"""
						SELECT * FROM {source_table_name}
							WHERE idx in (SELECT s.idx
												  FROM {source_table_name} s EXCEPT
												  SELECT tg.idx
												  FROM {target_table_name} tg);""")
				for i, row in enumerate(cursor):
					columns = list(row.keys())
					if len(row['comment_text']) <= 140:
						sentence = row['comment_text']
					# sentences = self._split_comment(row['comment_text'])
					# for idx, sentence in enumerate(sentences):
						clean_sentence = self._clean_sentence(sentence)
						embed_sentence, embed_meta = self._get_embedding_bytes(clean_sentence)
						data = dict(zip(columns, row))
						if sentence != "":
							data.update({
								'sentence_idx': 0,
								'sentence': sentence,
								'clean_sentence': clean_sentence,
								'serialized_array_meta': embed_meta,
								'array_bytes': embed_sentence
							})

							rows.append(data)

							i += 1

							if len(rows) % 500 == 0:
								self._insert_sentence(insert_conn, rows, target_table_name)
								rows = []

				self._insert_sentence(insert_conn, rows, target_table_name)

		except (Exception, psycopg2.DatabaseError) as error:
			print(error)
		finally:
			if conn:
				conn.close()
			if insert_conn:
				insert_conn.close()

	def _split_comment(self, text):
		text = emoji.get_emoji_regexp().sub(r'', text.encode().decode('utf8'))
		if len(text) < 140:
			return [text]
		else:
			sentences = re.split(r'(?<=[^А-Я].[. |?|! \n]) +(?=[А-Я \n])', text)
			return sentences

	def _clean_sentence(self, sentence):
		sentence = "".join(re.findall(r'(\w+|[!?.,:;\- ])', str(sentence)))
		clean_sent = re.compile(r"\s+").sub(" ", sentence)
		return clean_sent.strip().lower()

	def _get_embedding_bytes(self, sentence):
		if sentence:
			embedding = np.array(self.embed(sentence.strip().lower()))
			array_meta = json.dumps({'array_meta':
										 {'dtype': embedding.dtype.str,
										  'shape': embedding.shape}})
			return embedding.tobytes(), array_meta
		else:
			return None, None


if __name__ == "__main__":
	lukoshko_text = LukoshkoTextToEmbeddings(db_params.db_params,
											 db_params.table_name_posts)
	lukoshko_text.create_embeddings(db_params.db_params,
									'insta_news_comments',
									db_params.table_name_posts)
