package com.joe.learning.sample;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.util.construction.renderer.PipelineDotRenderer;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BeamApp {

	public interface OptionsWithInputText extends StreamingOptions {

		@Description("Input text to print.")
		@Default.String("My input text")
		String getInputText();

		void setInputText(String value);
	}

	public static class PrintFn<T> extends DoFn<T, T> {

		private static final long serialVersionUID = 1049273604640616517L;

		private String logPrefix;

		@ProcessElement
		public void processElement(@Element T ele) {
			log.info("{}{}", (Objects.isNull(logPrefix) ? "" : logPrefix), ele);
		}

		public PrintFn() {
			super();
		}

		public PrintFn(String logPrefix) {
			this();
			this.logPrefix = logPrefix;
		}

		public String getLogPrefix() {
			return logPrefix;
		}

	}

	static class LogOutput<T> extends DoFn<T, T> {

		private static final long serialVersionUID = 5951020792771389167L;

		private final String prefix;

		public LogOutput(String prefix) {
			this.prefix = prefix;
		}

		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			log.info(prefix + c.element());
			c.output(c.element());
		}
	}

	public static void main(String[] args) {
		log.info("started");

		// sample1(args);
		sample2(args);

		log.info("completed...");
	}

	private static void sample2(String[] args) {
		// pipeline options
		PipelineOptions options;
		options = PipelineOptionsFactory.create();

		// pipeline
		Pipeline pipeline = Pipeline.create(options);

		PCollection<PersonInfo> pcol1 = pipeline.apply("Load elements", Create.of(getPersonInfos()));

//		PCollection<PersonInfo> pcol2 = pcol1.apply("Print elements1",
//				MapElements.into(TypeDescriptor.of(PersonInfo.class)).via(x -> {
//					log.info("{}", x);
//					return x;
//				}));

		// PCollection<PersonInfo> pcol3 = pcol1.apply("Print elements2", ParDo.of(new
		// PrintFn<PersonInfo>()));

		PCollection<KV<Integer, PersonInfo>> pcol4 = pcol1
				.apply(WithKeys.of(new SerializableFunction<PersonInfo, Integer>() {
					private static final long serialVersionUID = 2524898666124949028L;

					@Override
					public Integer apply(PersonInfo obj) {
						return obj.getAge();
					}
				}));

		PCollection<Integer> pkeys = pcol4.apply(Keys.create()).apply("Print keys",
				ParDo.of(new PrintFn<Integer>("key - ")));

		PCollection<PersonInfo> pvalues = pcol4.apply(Values.create()).apply("Print values",
				ParDo.of(new PrintFn<PersonInfo>("value - ")));

		PCollection<KV<Integer, Iterable<PersonInfo>>> pcol5 = pcol4.apply(GroupByKey.create());

		pcol5.apply(Keys.create()).apply("Print keys group", ParDo.of(new PrintFn<Integer>("key - group - ")));
		pcol5.apply(Values.create()).apply("Print values group",
				ParDo.of(new PrintFn<Iterable<PersonInfo>>("key - group - ")));

		pcol5.apply(ParDo.of(new LogOutput<>("PCollection pairs after GroupIntoBatches transform: ")));

		String dotString = PipelineDotRenderer.toDotString(pipeline);
		log.info("MY GRAPH REPR: " + dotString);
		pipeline.run().waitUntilFinish();
	}

	@SuppressWarnings("unused")
	private static void sample1(String[] args) {
		// pipeline options
		PipelineOptions options;
		// options= PipelineOptionsFactory.create();
		options = PipelineOptionsFactory.fromArgs(args).withValidation().as(OptionsWithInputText.class);

		// Created pipleline using PipelineOptionsFactory
		Pipeline pipeline = Pipeline.create(options);

		PCollection<String> pcol1 = pipeline.apply("Create elements",
				Create.of(Arrays.asList("Hello", "World!", ((OptionsWithInputText) options).getInputText())));

		PCollection<String> pcol2 = pcol1.apply("Print elements", MapElements.into(TypeDescriptors.strings()).via(x -> {
			log.info("{}", x);
			return x;
		}));
		// Output file generate with the intial name of element which contatins in the
		// list and followed with .txt format to the specified local path
		// pcol2.apply(TextIO.write().to("out").withSuffix(".txt"));
		PrintFn<String> po = new PrintFn<String>();
		pcol2.apply(ParDo.of(po));
		pipeline.run().waitUntilFinish();
	}

	static List<PersonInfo> getPersonInfos() {
		List<PersonInfo> pis = new ArrayList<>();
		for (int i = 1; i <= 10; i++) {
			pis.add(new PersonInfo(String.format("%03d", i), String.format("name - %03d", i),
					String.format("description - %03d", i), 20 + (i / 3), String.format("email%03d@gmail.com", i),
					"9".repeat(10 - String.valueOf(i).length()) + String.valueOf(i)));
		}
		return pis;
	}

	@AllArgsConstructor
	@Data
	static class PersonInfo implements Serializable {
		private static final long serialVersionUID = 3390832098489883967L;
		private String id;
		private String name;
		private String description;
		private Integer age;
		private String email;
		private String phone;
	}
}