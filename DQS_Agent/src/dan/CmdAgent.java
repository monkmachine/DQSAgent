package dan;

import java.io.IOException;
import java.io.InputStream;

import com.ataccama.dqc.model.annotations.Algorithm;
import com.ataccama.dqc.model.annotations.AlgorithmCategory;
import com.ataccama.dqc.model.elements.data.AccessMode;
import com.ataccama.dqc.model.elements.data.ColumnInfo;
import com.ataccama.dqc.model.elements.data.IRecord;
import com.ataccama.dqc.model.elements.data.IRecordFormat;
import com.ataccama.dqc.model.elements.data.IRecordFormatListener;
import com.ataccama.dqc.model.elements.data.StandardRecordFormat;
import com.ataccama.dqc.model.elements.data.bindings.Binding;
import com.ataccama.dqc.model.elements.data.flow.IQueueInputEndpoint;
import com.ataccama.dqc.model.elements.data.flow.IQueueOutputEndpoint;
import com.ataccama.dqc.model.elements.data.flow.QueueBatcher;
import com.ataccama.dqc.model.elements.steps.ComplexStepBase;
import com.ataccama.dqc.model.elements.steps.EndPoint;
import com.ataccama.dqc.model.elements.steps.EndPointMapping;
import com.ataccama.dqc.model.elements.steps.IComplexStep;
import com.ataccama.dqc.model.environment.IAlgorithmContext;
import com.ataccama.dqc.model.messages.ModelMessage;
import com.ataccama.dqc.model.messages.ModelMessages;
import com.ataccama.dqc.model.messages.Severity;
import com.ataccama.dqc.model.validation.IValidationContext;
import com.ataccama.dqc.tasks.common.config.ComplexStepConfigBase;

@Algorithm(
        smallIcon="icons/SampleComplexStep.small.png",
        largeIcon="icons/SampleComplexStep.large.png",
        category=@AlgorithmCategory(
            value="category.custom",
            messageKey="Custom",
            smallIcon="icons/category.custom.small.png"))
public class CmdAgent extends ComplexStepConfigBase {
	/*ENDPOINTS*/
	private EndPoint in1 = new EndPoint(this, "in1", true, false, 1);
	private EndPoint out = new EndPoint(this, "out", false, false, 1);
	public EndPoint[] getEndpoints() {
		return new EndPoint[]{in1,  out};
	}
	/*ENDPOINTS*/

	public CmdAgent() {
		super(null);
		initListeners();
	}
	/*SYNCHRONIZATION OF INPUT AND OUTPUT END POINTS */
	private void initListeners() {
		IRecordFormatListener recListener = new InRecordListener();
		in1.addRecordFormatListener(recListener);
			}
	private class InRecordListener implements IRecordFormatListener {
		public void recordFormatChanged(IRecordFormat o, IRecordFormat n) {
			IRecordFormat in1Fmt = in1.getRecordFormat();
			if (in1Fmt != null ) {
				out.setRecordFormat(createCopy(in1Fmt));
			} else {
				out.setRecordFormat(null);
			}
		}
		private IRecordFormat createCopy(IRecordFormat or) {
			ColumnInfo[] cols = or.getColumns();
			StandardRecordFormat newFmt = new StandardRecordFormat();
			for (ColumnInfo c: cols) {
				newFmt.addColumn(c);
			}
			newFmt.lock();
			return newFmt;
		}
	}
	/*SYNCHRONIZATION OF INPUT AND OUTPUT END POINTS */


	@Override
	protected void validateStep(IValidationContext ctx) {
		super.validateStep(ctx);
		ModelMessages mms = ctx.getMessages();
		IRecordFormat in1Fmt = in1.getRecordFormat();
		if (in1Fmt == null ) {
			mms.addMessage(new ModelMessage(this, Severity.ERROR, "in1_and_in2_dont_have_same_format"));
		}
	}


	public IComplexStep createComplexStep(EndPointMapping[] epms,
			IAlgorithmContext arg1) throws Exception {
		return new TheInstance(findOutQueue(in1, epms),  findInQueue(out, epms, true));
	}


	/*INSTANCE*/
	private class TheInstance extends ComplexStepBase {
		private QueueBatcher outBatcher;
		private final IQueueOutputEndpoint in1QE;
		private final IRecordFormat outFmt = out.getRecordFormat();
		private final Binding[] outBindings;
		public TheInstance(IQueueOutputEndpoint in1QE,
				IQueueInputEndpoint outQE) {
			this.in1QE = in1QE;
			outBatcher = new QueueBatcher(outQE, 20);
			ColumnInfo[] outCols = outFmt.getColumns();
			this.outBindings = new Binding[outCols.length];
			for (int i = 0; i < outBindings.length; i++) {
				outBindings[i] = outFmt.createBinding(outCols[i].getName(), AccessMode.RW);
			}
		}



		public void run() throws Exception {
			IRecord[] recs;
			while ((recs = in1QE.getBatch()) != null) {
				for (IRecord r: recs) {
					outBatcher.addRecord(copyRecord(r));
				}
			}
			Runtime runtime = Runtime.getRuntime();
			try {
			    Process p1 = runtime.exec("cmd /c start G:\\TIB_dqs_12.6.3_win_x86_64\\runtime\\bin\\runcif.bat");
			    InputStream is = p1.getInputStream();
			    int i = 0;
			    while( (i = is.read() ) != -1) {
			        System.out.print((char)i);
			    }
			} catch(IOException ioException) {
			    System.out.println(ioException.getMessage() );
			}
			outBatcher.close();
		}


		private IRecord copyRecord(IRecord r) {
			IRecord copy = outFmt.createNewRecord();
			for (int i = 0; i < outBindings.length; i++) {
				outBindings[i].set(copy, outBindings[i].get(r));
			}
			return copy;
		}
	}


	/*INSTANCE*/

}


