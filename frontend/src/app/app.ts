import { Component } from '@angular/core';

import { JobBoard } from './job-board/job-board';

@Component({
  selector: 'app-root',
  imports: [JobBoard],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {}
