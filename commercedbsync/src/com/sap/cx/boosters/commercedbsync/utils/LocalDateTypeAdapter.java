package com.sap.cx.boosters.commercedbsync.utils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;


public class LocalDateTypeAdapter extends TypeAdapter<LocalDateTime>
{

	@Override
	public LocalDateTime read(JsonReader jsonReader) throws IOException
	{
		if (jsonReader.peek() == JsonToken.NULL)
		{
			jsonReader.nextNull();
			return null;
		}
		return ZonedDateTime.parse(jsonReader.nextString()).toLocalDateTime();
	}

	@Override
	public void write(final JsonWriter jsonWriter, final LocalDateTime localDate) throws IOException
	{
		if (localDate == null)
		{
			jsonWriter.nullValue();
			return;
		}
		jsonWriter.value(localDate.toString());
	}
}
