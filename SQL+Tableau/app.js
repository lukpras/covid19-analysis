console.log('Is this working?');

let viz;

//Add Share Link to Tableau Public in here
const url = "https://prod-apsoutheast-a.online.tableau.com/t/lukiprasetyo/views/psql-learn-heroku-store/Dashboard1/6c29c2dd-0bbf-4d75-aedf-c903b3900cbc/728115e7-3998-482e-9f32-e3e44be75661?:display_count=n&:showVizHome=n&:origin=viz_share_link";

const vizContainer = document.getElementById('vizContainer');
const options = {
    hideTabs: true,
    height: 1000,
    width: 1200,
    onFirstInteraction: function() {
        workbook = viz.getWorkbook();
        activeSheet = workbook.getActiveSheet();
        console.log("My dashboard is interactive");
    }
};

//create a function to generate the viz element
function initViz() {
    console.log('Executing the initViz function!');
    viz = new tableau.Viz(vizContainer, url, options);
}

// run the initViz function when the page loads
document.addEventListener("DOMContentLoaded", initViz);

const exportPDF = document.getElementById('exportPDF');
const exportImage = document.getElementById('exportImage');


//click on the pdf button to generate pdf of dashboard
function generatePDF() {
    viz.showExportPDFDialog()
}

exportPDF.addEventListener("click", function () {
    generatePDF();
  });

//click on image to generate image of dashboard
function generateImage() {
    viz.showExportImageDialog()
}

exportImage.addEventListener("click", function () {
    generateImage();
  });
